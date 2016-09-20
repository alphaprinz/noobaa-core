#!/bin/bash
echo "installing NooBaa"
PATH=/usr/local/noobaa:$PATH;
mkdir /usr/local/noobaa/logs

if [[ -d /usr/lib/systemd ]]; then
  echo "Systemd detected. Installing service"
  /usr/local/noobaa/node /usr/local/noobaa/src/agent/agent_linux_installer --repair
  systemctl enable noobaalocalservice
elif [[ -d /usr/share/upstart ]]; then
  echo "Upstart detected. Creating startup script"
  if [ -f /etc/init/noobaalocalservice.conf ]; then
    echo "Service already installed. Removing old service"
    service noobaalocalservice stop
    rm /etc/init/noobaalocalservice.conf
  fi
  cp /usr/local/noobaa/src/agent/noobaalocalservice.conf /etc/init/noobaalocalservice.conf
  service noobaalocalservice start
elif [[ -d /etc/init.d ]]; then
  echo "System V detected. Installing service"
  /usr/local/noobaa/node /usr/local/noobaa/src/agent/agent_linux_installer --repair
else
  echo "ERROR: Cannot detect init mechanism! Attempting to force service installation"
  if [ -f /etc/init/noobaalocalservice.conf ]
    service noobaalocalservice stop
    rm /etc/init/noobaalocalservice.conf
  fi
  /usr/local/noobaa/node /usr/local/noobaa/src/agent/agent_linux_installer --repair
  systemctl enable noobaalocalservice
  cp /usr/local/noobaa/src/agent/noobaalocalservice.conf /etc/init/noobaalocalservice.conf
  service noobaalocalservice restart
fi

echo "NooBaa installation complete"
