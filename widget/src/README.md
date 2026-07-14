# Widget Src

- `sdk`: 실제 서비스가 설치하는 iframe loader API
- `app/widget-*`: 서비스가 호스팅하는 widget UI
- `app/admin-*`: Boxer Web이 호스팅하는 admin UI
- `entries/widget`, `entries/admin`: 배포 주체별 Vite HTML entry

widget과 admin은 source workspace만 공유하고 `dist/widget`, `dist/admin`으로 분리 빌드한다.
