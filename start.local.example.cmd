@echo off

rem Copy this file to start.local.cmd and fill in local secrets.
rem start.local.cmd is ignored by Git and loaded automatically by start.cmd.

set "RUOYI_MEDIA_BASE_URL=http://10.200.77.58:43300"

set "RUOYI_MEDIA_MINIO_ENDPOINT=http://8.134.239.122:18084"
set "RUOYI_MEDIA_MINIO_ACCESS_KEY="
set "RUOYI_MEDIA_MINIO_SECRET_KEY="
set "RUOYI_MEDIA_MINIO_BUCKET=ruoyi-media"
