#!/bin/bash
# ══════════════════════════════════════════════════════
# سكريبت النفق الدائم — يعيد الاتصال تلقائياً
# ══════════════════════════════════════════════════════

echo "🌐 تشغيل النفق الدائم..."
echo "اضغط Ctrl+C للإيقاف"
echo ""

while true; do
    echo "$(date '+%H:%M:%S') — جارٍ الاتصال..."
    ssh -o StrictHostKeyChecking=no \
        -o ServerAliveInterval=30 \
        -o ServerAliveCountMax=3 \
        -R 80:localhost:8000 localhost.run

    echo "$(date '+%H:%M:%S') — انقطع الاتصال. إعادة الاتصال خلال 5 ثوانٍ..."
    sleep 5
done
