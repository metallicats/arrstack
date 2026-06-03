This will install the usual ARR stack as follows:

| Hostname                      | Forward Hostname | Port |
| ----------------------------- | ---------------- | ---- |
| aiostreams.yourdomain.com     | aiostreams       | 3000 |
| nzbdav.yourdomain.com         | nzbdav           | 3000 |
| prowlarr.yourdomain.com       | prowlarr         | 9696 |
| radarr.yourdomain.com         | radarr           | 7878 |
| sonarr.yourdomain.com         | sonarr           | 8989 |
| usenetstreamer.yourdomain.com | usenetstreamer   | 7000 |

This is specfically formatted for Usenet, so no QBitTorrent.

This utilizes NZBDav to chache and stream the files without downloading the full file. Very usefull if you have limited storage.
Specially now that storage is very expensive.

Clone the whole repo to your cloud instance and run setup.sh
