Instructions:
1. Create the folder `/etc/systemd/system/fractal.d`, and copy `fractal.config` in there. Update this configuration file as needed.
2. Copy `fractal.service` in `/etc/systemd/system/`. Update this file as needed (especially the `User` and `ExecStart` fields).
3. Start/check/stop the service with
```
sudo systemctl start fractal.service
systemctl status fractal.service
sudo systemctl stop fractal.service
```
