#!/usr/bin/env bash
# Installs remoteagent as a systemd service that starts at boot and restarts on crash.
# Usage: sudo ./install.sh /path/to/remoteagent-shaded.jar
set -euo pipefail

JAR_SRC="${1:?usage: install.sh <path-to-remoteagent-shaded.jar>}"
INSTALL_DIR=/opt/remoteagent
ENV_DIR=/etc/remoteagent
ENV_FILE="$ENV_DIR/remoteagent.env"
SERVICE_NAME=remoteagent
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $EUID -ne 0 ]]; then
  echo "run as root (sudo)" >&2
  exit 1
fi

if [[ ! -f "$JAR_SRC" ]]; then
  echo "jar not found: $JAR_SRC" >&2
  exit 1
fi

id -u remoteagent &>/dev/null || useradd --system --no-create-home --shell /usr/sbin/nologin remoteagent

mkdir -p "$INSTALL_DIR" "$ENV_DIR"
cp "$JAR_SRC" "$INSTALL_DIR/remoteagent.jar"
chown -R remoteagent:remoteagent "$INSTALL_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  cat > "$ENV_FILE" <<'EOF'
REMOTEAGENT_SERVER=ws://batchhost:8090/agent/ws
REMOTEAGENT_ID=changeme
REMOTEAGENT_HOSTNAME=changeme
REMOTEAGENT_TOKEN=changeme

# Extra options, whitespace-separated. Set a trust store here when the endpoints this agent
# calls (or the server itself, over wss://) use certificates the host JVM does not trust:
#   REMOTEAGENT_OPTS=--truststore=/etc/remoteagent/corporate-ca.jks --truststore-password=secret
REMOTEAGENT_OPTS=
EOF
  chmod 600 "$ENV_FILE"
  chown remoteagent:remoteagent "$ENV_FILE"
  echo "Wrote $ENV_FILE with placeholder values — edit it before starting the service."
fi

cp "$SCRIPT_DIR/remoteagent.service" /etc/systemd/system/remoteagent.service
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

echo "Installed and enabled for boot start."
echo "Next: edit $ENV_FILE, then run: systemctl start $SERVICE_NAME"
