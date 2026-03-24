# Local Official Datasets

이 프로젝트의 `근처 역` 열은 외부 유료 API나 스크래핑 없이, 사용자가 직접 내려받은 공식 파일만 로컬에서 읽도록 구성했습니다.

## 사용하는 파일

- 역 데이터: 루트에 둔 `*도시철도역사정보*.xlsx`
- 아파트 좌표 데이터: 주소정보누리집 등 공식 DB에서 내려받은 건물/좌표 파일

## 생성되는 파일

- `data/generated/subway-stations.json`
- `data/generated/apartment-coordinate-index.json`

이 두 파일은 런타임 전용 캐시라서 `.gitignore`에 포함했습니다.

## 명령어

역 데이터 전처리:

```powershell
npm run build:stations
```

아파트 좌표 인덱스 생성:

단일 파일에 주소와 좌표가 같이 있는 경우

```powershell
python scripts/build_apartment_coordinate_index.py --input "C:\path\to\official_file.txt"
```

건물 파일과 좌표 파일이 분리된 경우

```powershell
python scripts/build_apartment_coordinate_index.py --buildings "C:\path\to\building.txt" --coords "C:\path\to\coord.txt"
```

## 보안 / 비용 / 운영 원칙

- `근처 역` 계산에는 제3자 유료 API를 사용하지 않습니다.
- 주소/좌표 원본 파일은 로컬 디스크에서만 읽습니다.
- 생성 JSON도 로컬에서만 사용합니다.
- 역 거리 계산은 직선거리 기준입니다. 도보 경로 길이는 아닙니다.
