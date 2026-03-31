#!/bin/bash
echo "📦 تثبيت المتطلبات..."
pip install fastapi uvicorn pyquotex --quiet 2>/dev/null

echo "🌐 تنزيل Cloudflare Tunnel..."
if [ ! -f "$PREFIX/bin/cloudflared" ]; then
    wget -q "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64" \
         -O "$PREFIX/bin/cloudflared"
    chmod +x "$PREFIX/bin/cloudflared"
    echo "✅ cloudflared مثبت"
else
    echo "✅ cloudflared موجود"
fi

echo ""
echo "✅ جاهز!"
echo "▶️  نافذة 1: python bot.py"
echo "🌐 نافذة 2: cloudflared tunnel --url http://localhost:8000"
echo "👤 الأدمن:  https://رابطك/admin  | كلمة المرور: Admin@2024"
