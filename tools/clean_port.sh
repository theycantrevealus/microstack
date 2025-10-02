for port in 27117; do
  echo $(lsof -i ":$port")
  pid=$(ss -ltnp 2>/dev/null | grep ":$port " | awk -F',' '{print $2}' | awk '{print $1}' || true)
  echo $pid
  [ -z "$pid" ] && pid=$(lsof -t -i:$port 2>/dev/null || true)

  if [ -n "$pid" ]; then
    echo "🗑️ Killing PID $pid (port $port)"
    kill -9 "$pid" || true
  fi
done