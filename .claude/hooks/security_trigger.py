#!/usr/bin/env python3
import json
import sys
import re

AGENT_TRIGGERS = {
    "security-auditor": ["password", "auth", "token", "api_key", "secret"]
}

# 허용된 무료 공공 API 도메인 (화이트리스트)
ALLOWED_API_DOMAINS = [
    "apis.data.go.kr",          # 공공데이터포털 (무료)
    "api.vworld.kr",            # VWorld 국토정보 (무료)
    "openapi.seoul.go.kr",      # 서울 열린데이터광장 (무료)
    "open.assembly.go.kr",      # 국회 열린데이터 (무료)
    "www.law.go.kr",            # 법제처 (무료)
    "localhost",                # 로컬 개발
    "127.0.0.1",                # 로컬 개발
]

# 유료 API 도메인 (블랙리스트 — 감지 시 경고)
PAID_API_DOMAINS = [
    "maps.googleapis.com",      # Google Maps (유료)
    "api.openai.com",           # OpenAI (유료)
    "api.ncloud",               # 네이버 클라우드 (유료)
    "dapi.kakao.com",           # 카카오 API (일부 유료)
    "naveropenapi.apigw",       # 네이버 API (유료)
    "api.aws.amazon.com",       # AWS (유료)
    "azure.microsoft.com",      # Azure (유료)
    "api.mapbox.com",           # Mapbox (유료)
]

def check_paid_apis(tool_input):
    """코드에 유료 API 도메인이 포함되어 있는지 검사"""
    content = str(tool_input).lower()

    # URL 패턴 추출
    urls = re.findall(r'https?://[^\s"\'<>]+', content)
    domains = set()
    for url in urls:
        match = re.match(r'https?://([^/:]+)', url)
        if match:
            domains.add(match.group(1).lower())

    # 유료 API 블랙리스트 검사
    blocked = []
    for domain in domains:
        for paid in PAID_API_DOMAINS:
            if paid in domain:
                blocked.append(domain)

    if blocked:
        result = {
            "decision": "block",
            "reason": f"⛔ [유료 API 차단] 유료 API 도메인이 감지되었습니다: {', '.join(blocked)}\n"
                      f"이 프로젝트는 무료 공공 API만 사용합니다. 유료 API 사용이 필요하면 사용자에게 먼저 확인하세요.\n"
                      f"허용된 도메인: {', '.join(ALLOWED_API_DOMAINS)}"
        }
        print(json.dumps(result))
        return True

    # 허용 목록에 없는 외부 API 경고
    unknown = []
    for domain in domains:
        if domain in ("localhost", "127.0.0.1"):
            continue
        if not any(allowed in domain for allowed in ALLOWED_API_DOMAINS):
            unknown.append(domain)

    if unknown:
        print(f"\n⚠️ [API 모니터링] 허용 목록에 없는 외부 도메인 감지: {', '.join(unknown)}\n"
              f"무료 API인지 확인 후 ALLOWED_API_DOMAINS에 추가하세요.\n")

    return False


def suggest_agents(tool_input):
    suggestions = []
    content = str(tool_input).lower()

    for agent, triggers in AGENT_TRIGGERS.items():
        if any(trigger in content for trigger in triggers):
            suggestions.append(agent)

    if suggestions:
        print(f"\n💡 [시스템 감지] 보안 민감 단어가 감지되었습니다. '{', '.join(suggestions)}' 에이전트 사용을 고려해 보세요.\n")


if __name__ == "__main__":
    data = json.load(sys.stdin)
    if data.get("tool_name") in ["Edit", "Write"]:
        check_paid_apis(data.get("tool_input", {}))
        suggest_agents(data.get("tool_input", {}))
