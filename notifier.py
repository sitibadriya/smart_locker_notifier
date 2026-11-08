import firebase_admin
from firebase_admin import credentials, firestore, messaging
from google.cloud.firestore_v1 import FieldFilter
 
from datetime import datetime, timezone
import threading
import time
import hashlib
import re
 
# ============================================================
# SMART LOCKER NOTIFIER
# ESP32 -> Firestore -> Python -> FCM Android + Web
# ============================================================
 
# ================= FIREBASE =================
 
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
 
db = firestore.client()
 
activity_ref = db.collection("locker-activity")
devices_ref = db.collection("notification-devices")
notifications_ref = db.collection("notifications")
 
# ================= SETTINGS =================
 
FAIL_LIMIT = 3
FAIL_WINDOW = 60
 
WARNING_COOLDOWN = 300
 
LOCKER_DEFAULT = "L01"
 
# True  : token Web dikirimi pesan data-only. Notifikasi ditampilkan SATU kali
#         oleh firebase-messaging-sw.js (background) / Flutter (foreground).
#         Ini mencegah notifikasi ganda dari FCM SDK + Service Worker.
# False : perilaku lama (notification + webpush.notification).
WEB_DATA_ONLY = True
 
# Alert BAHAYA yang tidak pernah menerima RECOVERY dianggap basi setelah
# sekian detik, supaya kejadian berikutnya tidak diblokir selamanya.
ALERT_STALE_SECONDS = 1800
 
lock = threading.RLock()
initialized = False
 
failed_attempts = {}
 
# Cooldown per locker + jenis warning
last_warning = {}
 
# Alert aktif untuk dedup dan recovery
active_alerts = {}
 
# Dedup event yang sudah diproses
seen_event_ids = set()
 
# ================= HELPERS =================
 
def utc_now():
    return datetime.now(timezone.utc)
 
 
def text_value(value, fallback=""):
    if value is None:
        return fallback
    return str(value).strip()
 
 
def normalize(value):
    return (
        text_value(value)
        .upper()
        .replace("-", " ")
        .replace("_", " ")
    )
 
 
def safe_key(value):
    return re.sub(r"[^A-Z0-9]+", "_", normalize(value)).strip("_")
 
 
def make_event_id(document_id, data):
    supplied = data.get("eventId") or data.get("event_id")
 
    if supplied:
        return str(supplied)
 
    if document_id:
        return str(document_id)
 
    raw = "|".join([
        text_value(data.get("idLocker")),
        text_value(data.get("hasilAkses")),
        text_value(data.get("keterangan")),
        text_value(data.get("timestamp")),
    ])
 
    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:24]
 
 
def event_time(data):
    value = (
        data.get("timestamp")
        or data.get("waktu")
        or data.get("createdAt")
    )
 
    if hasattr(value, "to_datetime"):
        try:
            return value.to_datetime().astimezone(timezone.utc)
        except Exception:
            pass
 
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
 
    return utc_now()
 
 
def locker_id(data):
    return text_value(
        data.get("idLocker")
        or data.get("locker_id"),
        LOCKER_DEFAULT
    )
 
 
# ================= FCM PAYLOAD =================
 
def build_payload(
    title,
    body,
    category,
    locker,
    event_id,
    status="ACTIVE",
    action="",
    occurred_at=None,
    priority="NORMAL",
    alert_key=""
):
    occurred_at = occurred_at or utc_now()
 
    return {
        "event_id": str(event_id),
        "alert_key": str(alert_key or ""),
        "category": str(category),
        "level": str(category).lower(),
        "idLocker": str(locker),
        "locker_id": str(locker),
        "locker": str(locker),
        "title": str(title),
        "body": str(body),
        "occurred_at": occurred_at.isoformat(),
        "status": str(status),
        "action": str(action or ""),
        "priority": str(priority),
    }
 
 
# ================= FIRESTORE NOTIFICATION HISTORY =================

def save_notification_history(payload):
    """Simpan satu notifikasi agar Web dan Mobile bisa membaca riwayat realtime."""
    doc_id = text_value(payload.get("event_id")) or hashlib.sha256(
        repr(payload).encode("utf-8")
    ).hexdigest()[:24]

    document = dict(payload)
    document["createdAt"] = firestore.SERVER_TIMESTAMP
    document["read"] = False

    try:
        notifications_ref.document(doc_id).set(document, merge=True)
        print(f"[HISTORY] Tersimpan: {doc_id} ({payload.get('category')})")
        return True
    except Exception as exc:
        print(f"[HISTORY] Gagal menyimpan {doc_id}: {exc}")
        return False


# ================= FCM SENDER =================
 
def send_notification(
    title,
    body,
    category="INFO",
    locker=LOCKER_DEFAULT,
    event_id=None,
    status="ACTIVE",
    action="",
    occurred_at=None,
    priority=None,
    alert_key=""
):
    event_id = event_id or (
        f"{locker}-{int(time.time() * 1000)}"
    )
 
    priority = priority or (
        "HIGH" if category == "BAHAYA" else "NORMAL"
    )
 
    payload = build_payload(
        title=title,
        body=body,
        category=category,
        locker=locker,
        event_id=event_id,
        status=status,
        action=action,
        occurred_at=occurred_at,
        priority=priority,
        alert_key=alert_key
    )
 
    # Simpan riwayat terlebih dahulu; push dan history memakai event_id yang sama.
    save_notification_history(payload)

    sent = 0
    failed = 0
    invalid_tokens = []
 
    # Ambil device aktif
    try:
        devices = list(
            devices_ref.where(
                filter=FieldFilter("enabled", "==", True)
            ).stream()
        )
 
    except Exception as exc:
        print("[FCM] Gagal membaca devices:", exc)
        return {"sent": 0, "failed": 1}
 
    seen_tokens = set()
 
    for device in devices:
        device_data = device.to_dict() or {}
 
        token = device_data.get("token")
        platform = text_value(
            device_data.get("platform"),
            "unknown"
        ).lower()
 
        if not token:
            print("[FCM] Token kosong:", device.id)
            continue
 
        # Satu token = satu notifikasi, walaupun terdaftar di beberapa dokumen.
        if token in seen_tokens:
            print(f"[FCM] Token ganda dilewati: device={device.id}")
            continue
 
        seen_tokens.add(token)
 
        is_web = platform.startswith("web") or platform == "browser"
        is_high = priority == "HIGH"
 
        urgency_headers = {
            "Urgency": "high" if is_high else "normal",
            "TTL": "3600"
        }
 
        if is_web and WEB_DATA_ONLY:
            # Data-only: tampilan sepenuhnya dikendalikan Service Worker.
            message = messaging.Message(
                data=payload,
                webpush=messaging.WebpushConfig(
                    headers=urgency_headers
                ),
                token=token
            )
 
        elif is_web:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body
                ),
                data=payload,
                webpush=messaging.WebpushConfig(
                    headers=urgency_headers,
                    notification=messaging.WebpushNotification(
                        title=title,
                        body=body,
                        icon="/icons/Icon-192.png",
                        tag=str(event_id),
                        renotify=False
                    )
                ),
                token=token
            )
 
        else:
            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body
                ),
                data=payload,
                android=messaging.AndroidConfig(
                    priority="high" if is_high else "normal",
                    notification=messaging.AndroidNotification(
                        channel_id="smart_locker_alerts",
                        priority="high" if is_high else "default"
                    )
                ),
                token=token
            )
 
        try:
            response = messaging.send(message)
            sent += 1
 
            print(
                f"[FCM] Terkirim ({platform}) "
                f"device={device.id} id={response}"
            )
 
        except messaging.UnregisteredError:
            failed += 1
            invalid_tokens.append(device.id)
 
            print(
                f"[FCM] Token invalid "
                f"({platform}) device={device.id}"
            )
 
        except Exception as exc:
            failed += 1
 
            print(
                f"[FCM] Gagal ({platform}) "
                f"device={device.id}: {exc}"
            )
 
    # Nonaktifkan token invalid
    for device_id in invalid_tokens:
        try:
            devices_ref.document(device_id).update({
                "enabled": False,
                "disabledAt": firestore.SERVER_TIMESTAMP
            })
 
        except Exception as exc:
            print(
                "[FCM] Gagal menonaktifkan token:",
                device_id,
                exc
            )
 
    print(
        f"[FCM] Total sukses={sent}, "
        f"gagal={failed}, kategori={category}"
    )
 
    return {
        "sent": sent,
        "failed": failed
    }
 
 
# ================= WARNING =================
 
def send_warning(
    locker,
    reason,
    event_id=None,
    action=None,
    occurred_at=None,
    warning_key=None
):
    warning_key = safe_key(warning_key or reason)
    cooldown_key = (locker, warning_key)
 
    now = time.time()
 
    with lock:
        previous = last_warning.get(cooldown_key, 0)
 
        if now - previous < WARNING_COOLDOWN:
            remaining = int(
                WARNING_COOLDOWN - (now - previous)
            )
 
            print(
                f"[WASPADA] Cooldown aktif: "
                f"{locker}/{warning_key} ({remaining}s)"
            )
 
            return False
 
        last_warning[cooldown_key] = now
 
    event_id = event_id or (
        f"{locker}-warning-{warning_key}-{int(now)}"
    )
 
    result = send_notification(
        title=f"⚠️ WASPADA — Locker {locker}",
        body=reason,
        category="WASPADA",
        locker=locker,
        event_id=event_id,
        status="ACTIVE",
        action=action or f"Periksa locker {locker}.",
        occurred_at=occurred_at,
        priority="NORMAL",
        alert_key=warning_key
    )
 
    return result["sent"] > 0
 
 
# ================= DANGER =================
 
def send_danger(
    locker,
    reason,
    alert_key=None,
    event_id=None,
    action=None,
    occurred_at=None
):
    alert_key = safe_key(alert_key or reason)
    key = (locker, alert_key)
 
    with lock:
        existing = active_alerts.get(key)
 
        if existing is not None:
            umur = (utc_now() - existing["started_at"]).total_seconds()
 
            if umur < ALERT_STALE_SECONDS:
                print(
                    f"[BAHAYA] Alert duplikat: "
                    f"{locker}/{alert_key}"
                )
                return False
 
            print(
                f"[BAHAYA] Alert lama basi ({int(umur)}s) "
                f"tanpa RECOVERY; dikirim ulang: {locker}/{alert_key}"
            )
 
        active_alerts[key] = {
            "started_at": utc_now(),
            "event_id": event_id
        }
 
    event_id = event_id or (
        f"{locker}-danger-{alert_key}-{int(time.time())}"
    )
 
    result = send_notification(
        title=f"🚨 BAHAYA — Locker {locker}",
        body=reason,
        category="BAHAYA",
        locker=locker,
        event_id=event_id,
        status="ACTIVE",
        action=action or f"Segera periksa locker {locker}.",
        occurred_at=occurred_at,
        priority="HIGH",
        alert_key=alert_key
    )
 
    if result["sent"] == 0:
        with lock:
            active_alerts.pop(key, None)
 
        return False
 
    return True
 
 
# ================= RECOVERY =================
 
def send_recovery(
    locker,
    reason,
    alert_key=None,
    event_id=None,
    action=None,
    occurred_at=None
):
    normalized_key = (
        safe_key(alert_key) if alert_key else None
    )
 
    with lock:
        matching = [
            key for key in active_alerts
            if key[0] == locker
            and (
                normalized_key is None
                or key[1] == normalized_key
            )
        ]
 
        if not matching:
            print(
                f"[RECOVERY] Tidak ada alert aktif "
                f"untuk {locker}; diabaikan."
            )
            return False
 
        recovery_keys = list(matching)
 
    recovery_id = event_id or (
        f"{locker}-recovery-{int(time.time())}"
    )
 
    result = send_notification(
        title=f"✅ RECOVERY — Locker {locker}",
        body=reason,
        category="RECOVERY",
        locker=locker,
        event_id=recovery_id,
        status="RECOVERED",
        action=action or f"Locker {locker} kembali aman.",
        occurred_at=occurred_at,
        priority="NORMAL",
        alert_key=normalized_key or ""
    )
 
    # Tutup alert setelah recovery berhasil terkirim
    if result["sent"] > 0:
        with lock:
            for key in recovery_keys:
                active_alerts.pop(key, None)
 
        return True
 
    return False
 
 
# ================= CLASSIFICATION =================
 
DANGER_PATTERNS = [
    "TAMPER",
    "PAKSA",
    "FORCED",
    "SOLENOID FAILURE",
    "SOLENOID GAGAL",
    "DOOR UNSAFE",
    "PINTU TIDAK AMAN",
    "PINTU DIBUKA PAKSA"
]
 
WARNING_PATTERNS = [
    "PINTU TERLALU LAMA",
    "DOOR OPEN TOO LONG",
    "OPEN TOO LONG",
    "ESP32 OFFLINE",
    "DEVICE OFFLINE",
    "HEARTBEAT LOST"
]
 
RECOVERY_PATTERNS = [
    "RECOVERY",
    "NORMAL KEMBALI",
    "KONDISI NORMAL",
    "KEMBALI NORMAL",
    "PINTU AMAN",
    "LOCKER AMAN",
    "DEVICE ONLINE",
    "ESP32 ONLINE"
]
 
REGISTRATION_SUCCESS_PATTERNS = [
    "FINGERPRINT REGISTERED",
    "REGISTRASI FINGERPRINT BERHASIL",
    "FINGERPRINT BERHASIL DIDAFTARKAN",
    "PENDAFTARAN FINGERPRINT BERHASIL",
    "ENROLLMENT SUCCESS",
    "ENROLL SUCCESS",
    "REGISTER SUCCESS"
]
 
 
def contains_any(text, patterns):
    return any(pattern in text for pattern in patterns)
 
 
def classify_activity(data):
    hasil = normalize(data.get("hasilAkses"))
    keterangan = normalize(data.get("keterangan"))
 
    event_type = normalize(
        data.get("eventType")
        or data.get("event_type")
    )
 
    category = normalize(
        data.get("category")
        or data.get("kategori")
    )
 
    combined = " ".join([
        hasil,
        keterangan,
        event_type,
        category
    ])
 
    # 1. INFO khusus pendaftaran fingerprint
    if (
        category == "REGISTER SUCCESS"
        or event_type in {
            "FINGERPRINT REGISTERED",
            "REGISTER FINGERPRINT",
            "ENROLL SUCCESS",
            "REGISTRATION SUCCESS"
        }
        or contains_any(combined, REGISTRATION_SUCCESS_PATTERNS)
    ):
        return "REGISTER_SUCCESS"
 
    # 2. Recovery harus diperiksa sebelum warning/danger
    if (
        category == "RECOVERY"
        or hasil in {"RECOVERY", "PULIH"}
        or contains_any(combined, RECOVERY_PATTERNS)
    ):
        return "RECOVERY"
 
    # 3. Label eksplisit dari ESP32 selalu menang atas pencocokan teks.
    #    (ESP32 menulis hasilAkses = "BAHAYA" / "PERINGATAN")
    if category == "BAHAYA" or hasil == "BAHAYA":
        return "BAHAYA"
 
    if category == "WASPADA" or hasil in {"PERINGATAN", "WASPADA"}:
        return "WASPADA"
 
    # 4. Pencocokan teks (untuk event tanpa label eksplisit)
    if contains_any(combined, DANGER_PATTERNS):
        return "BAHAYA"
 
    if contains_any(combined, WARNING_PATTERNS):
        return "WASPADA"
 
    # 5. Reset locker: History saja, tanpa push
    if hasil in {
        "DISETUJUI",
        "RESET",
        "RESET BERHASIL"
    }:
        return "IGNORE"
 
    # 6. Akses berhasil biasa: History saja, tanpa push
    if hasil in {
        "DIIZINKAN",
        "DIIJINKAN",
        "ACCESS GRANTED",
        "SUCCESS",
        "BERHASIL"
    }:
        return "INFO"
 
    # 7. Akses gagal
    # Variasi teks penolakan dari ESP32/app tetap dihitung sebagai gagal.
    failure_terms = (
        "DITOLAK", "DENIED", "FAILED", "GAGAL",
        "TIDAK COCOK", "NOT MATCH", "NO MATCH",
        "TIDAK DIKENALI", "UNKNOWN FINGER"
    )
    if hasil in {
        "DITOLAK", "TIDAK DIIZINKAN", "DENIED",
        "FAILED", "GAGAL", "AKSES DITOLAK",
        "FINGERPRINT DITOLAK", "SIDIK JARI DITOLAK"
    } or contains_any(combined, failure_terms):
        return "FAIL"
 
    return "IGNORE"
 
 
# ================= FIRESTORE HANDLER =================
 
def on_activity_snapshot(
    col_snapshot,
    changes,
    read_time
):
    global initialized
 
    with lock:
        if not initialized:
            initialized = True
 
            print(
                "[INIT] Snapshot awal dilewati; "
                "dokumen lama tidak dikirim ulang. Menunggu event baru."
            )
 
            return
 
    for change in changes:
        if change.type.name != "ADDED":
            continue
 
        document_id = change.document.id
        data = change.document.to_dict() or {}
 
        event_id = make_event_id(document_id, data)
 
        with lock:
            if event_id in seen_event_ids:
                print("[DEDUP] Event sudah diproses:", event_id)
                continue
 
            seen_event_ids.add(event_id)
 
        locker = locker_id(data)
 
        hasil = normalize(data.get("hasilAkses"))
 
        keterangan = text_value(
            data.get("keterangan"),
            "Kondisi locker terdeteksi."
        )
 
        nama = text_value(
            data.get("namaSantri"),
            "Pengguna"
        )
 
        occurred_at = event_time(data)
 
        print(
            f"[EVENT] id={event_id} "
            f"locker={locker} "
            f"hasil={hasil} "
            f"keterangan={keterangan}"
        )
 
        category = classify_activity(data)
 
        # INFO: fingerprint berhasil didaftarkan
        if category == "REGISTER_SUCCESS":
            send_notification(
                title=f"INFO — Locker {locker}",
                body="Fingerprint berhasil didaftarkan.",
                category="INFO",
                locker=locker,
                event_id=event_id,
                status="SUCCESS",
                action=f"Fingerprint baru terdaftar pada locker {locker}.",
                occurred_at=occurred_at,
                priority="NORMAL"
            )
 
            print("[INFO] Fingerprint berhasil didaftarkan:", locker)
            continue
 
        # INFO: akses berhasil hanya masuk History
        if category == "INFO":
            print(
                f"[INFO] Akses berhasil ({nama}); "
                "History saja, tanpa push."
            )
            continue
 
        # Fingerprint gagal
        if category == "FAIL":
            now = time.time()
 
            attempts = failed_attempts.setdefault(locker, [])
 
            attempts[:] = [
                timestamp
                for timestamp in attempts
                if now - timestamp <= FAIL_WINDOW
            ]
 
            attempts.append(now)
 
            print(
                f"[FAIL] {locker}: "
                f"{len(attempts)}/{FAIL_LIMIT} "
                f"dalam {FAIL_WINDOW}s"
            )
 
            if len(attempts) >= FAIL_LIMIT:
                warning_event_id = (
                    f"{locker}-fingerprint-{int(now)}"
                )
 
                send_warning(
                    locker,
                    f"Fingerprint gagal {len(attempts)} kali "
                    f"dalam {FAIL_WINDOW} detik.",
                    event_id=warning_event_id,
                    action=f"Periksa aktivitas fingerprint locker {locker}.",
                    occurred_at=occurred_at,
                    warning_key="FINGERPRINT_FAILED"
                )
 
                attempts.clear()
 
            continue
 
        # WASPADA
        if category == "WASPADA":
            warning_key = (
                data.get("alertKey")
                or data.get("alert_key")
                or keterangan
            )
 
            send_warning(
                locker,
                keterangan,
                event_id=event_id,
                action=data.get("action")
                or f"Periksa locker {locker}.",
                occurred_at=occurred_at,
                warning_key=warning_key
            )
 
            continue
 
        # BAHAYA
        if category == "BAHAYA":
            send_danger(
                locker,
                keterangan,
                alert_key=(
                    data.get("alertKey")
                    or data.get("alert_key")
                    or keterangan
                ),
                event_id=event_id,
                action=data.get("action")
                or f"Segera periksa locker {locker}.",
                occurred_at=occurred_at
            )
 
            continue
 
        # RECOVERY
        if category == "RECOVERY":
            send_recovery(
                locker,
                keterangan,
                alert_key=(
                    data.get("alertKey")
                    or data.get("alert_key")
            ),
                event_id=event_id,
                action=data.get("action")
                or f"Pastikan locker {locker} aman.",
                occurred_at=occurred_at
            )
 
            continue
 
        print("[IGNORE] Event tidak dikenali:", event_id)
 
 
# ================= START =================
 
def main():
    watch = activity_ref.on_snapshot(
        on_activity_snapshot
    )
 
    print("====================================")
    print(" SMART LOCKER NOTIFIER AKTIF")
    print("====================================")
    print("Monitoring Firestore: locker-activity")
    print("Android + Web FCM aktif")
    print(f"Fingerprint gagal: {FAIL_LIMIT}x/{FAIL_WINDOW}s")
    print(f"Cooldown WASPADA: {WARNING_COOLDOWN}s")
    print("INFO push: fingerprint berhasil terdaftar")
    print("Akses berhasil: History saja")
    print("Ctrl+C untuk menghentikan")
 
    try:
        while True:
            time.sleep(1)
 
    except KeyboardInterrupt:
        watch.unsubscribe()
        print("Notifier dihentikan.")
 
 
if __name__ == "__main__":
    main()