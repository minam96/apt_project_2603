# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

국토교통부 공공 API를 활용한 아파트 실거래가 조회 웹 대시보드.
프론트엔드(index.html 단일파일 ~4700줄) + 백엔드 BFF(server.js ~3000줄, MCP stdio 연동).

## 실행 명령어

```bash
# 서버 실행 (포트 3000)
node server.js          # 또는 npm start

# Vite 개발 서버 (포트 5173, server.js 프록시 경유)
npm run dev

# 빌드
npm run build           # vite build → dist/

# 로컬 데이터 전처리
npm run build:stations      # 도시철도역사정보 xlsx → data/generated/subway-stations.json
npm run build:apt-coords    # 공식 주소 DB → data/generated/apartment-coordinate-index.json

# 구문 검증
node --check server.js
python -m py_compile scripts/build_station_dataset.py
```

## 아키텍처

```
브라우저 (index.html)
  ↓ fetch /api/*
server.js (Node.js BFF, 포트 3000)
  ├─ MCP stdio ──→ _ref_real-estate-mcp (Python)
  │                  └─ data.go.kr API (실거래/전월세/건축물대장)
  ├─ 직접 HTTP ──→ data.go.kr API (raw XML 병합, 건축HUB, KAPT)
  ├─ 로컬 파일 ──→ data/generated/*.json (역/좌표)
  └─ region_codes.txt ──→ 지역코드 카탈로그
```

**핵심 데이터 흐름**: MCP 도구 호출로 거래 데이터를 가져온 뒤, raw XML API를 직접 호출해 메타데이터(법정동코드, 지번 등)를 병합. 이 병합 결과로 건축물대장 조회와 근처 역 계산이 가능해짐.

### server.js 주요 구조

- `MCP_ROUTES` 객체: URL 경로 → MCP 도구명/종류 매핑
- `handleMcpRoute()`: 거래/전월세 데이터 조회 (MCP + raw XML 병합 + 5분 캐시)
- `handleBuilding()`: 건축물대장 조회 (건축HUB 상태 자동 감지/비활성화)
- `handleListingGrid()`: 사업성분석 (KAPT 단지목록 + 건축HUB 종합)
- `enrichTradeItems()`: 거래 항목에 세대수/준공년월/근처 역 보강
- `ensureMcpConnected()`: MCP 서버 자동 연결/재연결

### index.html 구조

단일 파일에 HTML + CSS + JS 인라인. 5개 탭:
1. **실거래가** — 부동산 유형별 매매 테이블 + 신고가 감지 + 사이드바(요약/TOP10/지역별)
2. **전월세** — 전세/월세 필터 + 보증금/월세 테이블
3. **시세추이** — 최대 6개 단지 Canvas 라인 차트 (단지+지역+평형 단위)
4. **사업성분석** — KAPT 단지 + 건축HUB 데이터 기반 테이블
5. **매물검색** — 네이버 검색 연동 + 인기 단지 카드

## 환경 설정

```bash
# .env (서버 전용, git 제외)
MOLIT_API_KEY=data.go.kr_Decoding_키
# 또는
DATA_GO_KR_API_KEY=같은_키
```

API 키는 server.js에서만 읽고, 브라우저에는 절대 노출하지 않음.
`/api/config`는 `connected: boolean` + `datasets` 상태만 반환.

## 코드 규칙

- **server.js**: `@modelcontextprotocol/sdk`만 외부 의존성. 그 외 Node.js 내장 모듈만 사용.
- **index.html**: CSS/JS 모두 인라인. 외부 라이브러리 없음 (Google Fonts 제외).
- **면적 변환**: `Math.round(㎡ / 3.305)` 로 평 계산.
- **가격 표시**: 만원 단위 정수 → `formatPrice()`로 `74.5억`, `1억 2천` 형태 변환.
- **XSS 방지**: innerHTML 삽입 시 반드시 `esc()` 함수로 이스케이프.
- **근처 역 계산**: 직선거리(haversine) 기준. 외부 지오코딩 API 사용 금지.
- **Windows 환경**: `python3` 대신 `python` 사용.

## 로컬 데이터 파이프라인

역 데이터와 아파트 좌표는 외부 API 없이 로컬 공식 파일만 사용:
- 역: `전체_도시철도역사정보_*.xlsx` → `npm run build:stations`
- 좌표: 주소정보누리집 공식 DB → `npm run build:apt-coords --input <파일경로>`
- 생성 파일(`data/generated/`)과 원본(`data/raw/`)은 `.gitignore` 대상

## .gitignore 주요 항목

`.env`, `.mcp.json`, `tasks/`, `_ref_*/`, `data/raw/`, `data/generated/`, `node_modules/`

## gstack 스킬

웹 브라우징은 항상 `/browse` 사용. `mcp__claude-in-chrome__*` 툴 사용 금지.
스킬 오류 시: `cd .claude/skills/gstack && ./setup`

| 스킬 | 용도 |
|------|------|
| `/browse` | 브라우저 자동화 (로컬 확인) |
| `/plan-eng-review` | 아키텍처/엣지케이스 설계 |
| `/review` | 코드 리뷰 |
| `/qa` | QA 테스트 |
| `/ship` | 배포 |

## 오케스트레이터 원칙

### 작업 전후 루프

- 작업 시작 전: `tasks/lessons.md` (과거 실수), `tasks/todo.md` (진행 상황) 읽기
- 작업 완료 후: 새 교훈 추가, TODO 업데이트

### 서브 에이전트 호출

| 트리거 | 에이전트 |
|--------|----------|
| Edit/Write 시 보안 민감 단어 감지 (Hook) | `security-auditor` |
| 코드 변경 완료 시 (Stop Hook 안내) | `pr-reviewer` |
| 사용자 명시 요청 | 해당 에이전트 즉시 호출 |

호출 시 반드시 Task/Input/Output 구조 사용.

### 아키텍처 변경 시

코드 작성 전 PLAN 출력: 수정 파일, 리스크, 접근 방식.
