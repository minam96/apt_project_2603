#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
COPILOT_LOG_DIR = ROOT / "logs" / "copilot"
TOOL_GUARD_LOG_DIR = ROOT / ".github" / "logs" / "copilot" / "tool-guardian"
AUTO_COMMIT_APPROVAL_FILE = ROOT / ".claude" / "auto-commit.approved"


TOOL_GUARD_PATTERNS = [
    ("destructive_file_ops", "critical", r"rm -rf /", "Use targeted 'rm' on specific paths instead of root"),
    ("destructive_file_ops", "critical", r"rm -rf ~", "Use targeted 'rm' on specific paths instead of home directory"),
    ("destructive_file_ops", "critical", r"rm -rf \.", "Use targeted 'rm' on specific files instead of current directory"),
    ("destructive_file_ops", "critical", r"rm -rf \.\.", "Never remove parent directories recursively"),
    ("destructive_file_ops", "critical", r"(rm|del|unlink).*\.env", "Use 'mv' to back up .env files before removing"),
    ("destructive_file_ops", "critical", r"(rm|del|unlink).*\.git[^i]", "Never delete .git directly; use git commands instead"),
    ("destructive_git_ops", "critical", r"git push --force.*(main|master)", "Use 'git push --force-with-lease' or push to a feature branch"),
    ("destructive_git_ops", "critical", r"git push -f.*(main|master)", "Use 'git push --force-with-lease' or push to a feature branch"),
    ("destructive_git_ops", "high", r"git reset --hard", "Use 'git stash' or 'git reset --soft' to preserve work"),
    ("destructive_git_ops", "high", r"git clean -fd", "Use 'git clean -n' first to preview deletions"),
    ("database_destruction", "critical", r"DROP TABLE", "Use a reversible migration or backup before destructive changes"),
    ("database_destruction", "critical", r"DROP DATABASE", "Create a backup first and revoke DROP privileges where possible"),
    ("database_destruction", "critical", r"TRUNCATE", "Use a scoped DELETE with a WHERE clause when possible"),
    ("database_destruction", "high", r"DELETE FROM [a-zA-Z_]+ *;", "Add a WHERE clause before deleting rows"),
    ("permission_abuse", "high", r"chmod 777", "Use 'chmod 755' for directories or 'chmod 644' for files"),
    ("permission_abuse", "high", r"chmod -R 777", "Use specific permissions and limit recursive scope"),
    ("network_exfiltration", "critical", r"curl.*\|.*bash", "Download first, review the script, then execute it"),
    ("network_exfiltration", "critical", r"wget.*\|.*sh", "Download first, review the script, then execute it"),
    ("network_exfiltration", "high", r"curl.*--data.*@", "Review exactly what data is being uploaded before sending"),
    ("system_danger", "high", r"sudo ", "Avoid sudo unless absolutely necessary"),
    ("system_danger", "high", r"npm publish", "Use 'npm publish --dry-run' first"),
]

GOVERNANCE_PATTERNS = [
    (r"send\s+(all|every|entire)\s+\w+\s+to\s+", "data_exfiltration", 0.8, "Bulk data transfer"),
    (r"export\s+.*\s+to\s+(external|outside|third[_-]?party)", "data_exfiltration", 0.9, "External export"),
    (r"curl\s+.*\s+-d\s+", "data_exfiltration", 0.7, "HTTP POST with data"),
    (r"upload\s+.*\s+(credentials|secrets|keys)", "data_exfiltration", 0.95, "Credential upload"),
    (r"(sudo|as\s+root|admin\s+access|runas\s+/user)", "privilege_escalation", 0.8, "Elevated privileges"),
    (r"chmod\s+777", "privilege_escalation", 0.9, "World-writable permissions"),
    (r"add\s+.*\s+(sudoers|administrators)", "privilege_escalation", 0.95, "Adding admin access"),
    (r"(rm\s+-rf\s+/|del\s+/[sq]|format\s+c:)", "system_destruction", 0.95, "Destructive command"),
    (r"(drop\s+database|truncate\s+table|delete\s+from\s+\w+\s*(;|\s*$))", "system_destruction", 0.9, "Database destruction"),
    (r"wipe\s+(all|entire|every)", "system_destruction", 0.9, "Mass deletion"),
    (r"ignore\s+(previous|above|all)\s+(instructions?|rules?|prompts?)", "prompt_injection", 0.9, "Instruction override"),
    (r"you\s+are\s+now\s+(a|an)\s+(assistant|ai|bot|system|expert|language\s+model)\b", "prompt_injection", 0.7, "Role reassignment"),
    (r"(^|\n)\s*system\s*:\s*you\s+are", "prompt_injection", 0.6, "System prompt injection"),
    (r"(api[_-]?key|secret[_-]?key|password|token)\s*[:=]\s*['\"]?\w{8,}", "credential_exposure", 0.9, "Possible hardcoded credential"),
    (r"(aws_access_key|AKIA[0-9A-Z]{16})", "credential_exposure", 0.95, "AWS key exposure"),
]

SECRETS_PATTERNS = [
    ("AWS_ACCESS_KEY", "critical", r"AKIA[0-9A-Z]{16}"),
    ("AWS_SECRET_KEY", "critical", r"aws_secret_access_key\s*[:=]\s*['\"]?[A-Za-z0-9/+=]{40}"),
    ("GCP_SERVICE_ACCOUNT", "critical", r"\"type\"\s*:\s*\"service_account\""),
    ("GCP_API_KEY", "high", r"AIza[0-9A-Za-z_-]{35}"),
    ("AZURE_CLIENT_SECRET", "critical", r"azure[_-]?client[_-]?secret\s*[:=]\s*['\"]?[A-Za-z0-9_~.-]{34,}"),
    ("GITHUB_PAT", "critical", r"ghp_[0-9A-Za-z]{36}"),
    ("GITHUB_OAUTH", "critical", r"gho_[0-9A-Za-z]{36}"),
    ("GITHUB_APP_TOKEN", "critical", r"ghs_[0-9A-Za-z]{36}"),
    ("GITHUB_REFRESH_TOKEN", "critical", r"ghr_[0-9A-Za-z]{36}"),
    ("GITHUB_FINE_GRAINED_PAT", "critical", r"github_pat_[0-9A-Za-z_]{82}"),
    ("PRIVATE_KEY", "critical", r"-----BEGIN (RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----"),
    ("PGP_PRIVATE_BLOCK", "critical", r"-----BEGIN PGP PRIVATE KEY BLOCK-----"),
    ("GENERIC_SECRET", "high", r"(secret|token|password|passwd|pwd|api[_-]?key|apikey|access[_-]?key|auth[_-]?token|client[_-]?secret)\s*[:=]\s*['\"]?[A-Za-z0-9_/+=~.-]{8,}"),
    ("CONNECTION_STRING", "high", r"(mongodb(\+srv)?|postgres(ql)?|mysql|redis|amqp|mssql)://[^\s'\"]{10,}"),
    ("BEARER_TOKEN", "medium", r"[Bb]earer\s+[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}"),
    ("SLACK_TOKEN", "high", r"xox[baprs]-[0-9]{10,}-[0-9A-Za-z-]+"),
    ("SLACK_WEBHOOK", "high", r"https://hooks\.slack\.com/services/T[0-9A-Z]{8,}/B[0-9A-Z]{8,}/[0-9A-Za-z]{24}"),
    ("DISCORD_TOKEN", "high", r"[MN][A-Za-z0-9]{23,}\.[A-Za-z0-9_-]{6}\.[A-Za-z0-9_-]{27,}"),
    ("TWILIO_API_KEY", "high", r"SK[0-9a-fA-F]{32}"),
    ("SENDGRID_API_KEY", "high", r"SG\.[0-9A-Za-z_-]{22}\.[0-9A-Za-z_-]{43}"),
    ("STRIPE_SECRET_KEY", "critical", r"sk_live_[0-9A-Za-z]{24,}"),
    ("STRIPE_RESTRICTED_KEY", "high", r"rk_live_[0-9A-Za-z]{24,}"),
    ("NPM_TOKEN", "high", r"npm_[0-9A-Za-z]{36}"),
    ("JWT_TOKEN", "medium", r"eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"),
    ("INTERNAL_IP_PORT", "medium", r"(^|[^.0-9])(10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}|192\.168\.[0-9]{1,3}\.[0-9]{1,3}):[0-9]{2,5}([^0-9]|$)"),
]

DEFAULT_BLOCKED_LICENSES = [
    "GPL-2.0",
    "GPL-2.0-only",
    "GPL-2.0-or-later",
    "GPL-3.0",
    "GPL-3.0-only",
    "GPL-3.0-or-later",
    "AGPL-1.0",
    "AGPL-3.0",
    "AGPL-3.0-only",
    "AGPL-3.0-or-later",
    "LGPL-2.0",
    "LGPL-2.1",
    "LGPL-2.1-only",
    "LGPL-2.1-or-later",
    "LGPL-3.0",
    "LGPL-3.0-only",
    "LGPL-3.0-or-later",
    "SSPL-1.0",
    "EUPL-1.1",
    "EUPL-1.2",
    "OSL-3.0",
    "CPAL-1.0",
    "CPL-1.0",
    "CC-BY-SA-4.0",
    "CC-BY-NC-4.0",
    "CC-BY-NC-SA-4.0",
]

TEXT_SUFFIXES = {
    ".md", ".txt", ".json", ".yaml", ".yml", ".xml", ".toml", ".ini", ".cfg", ".conf",
    ".sh", ".bash", ".zsh", ".ps1", ".bat", ".cmd", ".py", ".rb", ".js", ".ts", ".jsx",
    ".tsx", ".go", ".rs", ".java", ".kt", ".cs", ".cpp", ".c", ".h", ".php", ".swift",
    ".scala", ".r", ".lua", ".pl", ".ex", ".exs", ".hs", ".ml", ".html", ".css", ".scss",
    ".less", ".svg", ".sql", ".graphql", ".proto", ".env", ".properties",
}

TEXT_FILENAMES = {"Dockerfile", "Makefile", "Vagrantfile", "Gemfile", "Rakefile"}
SKIP_SECRET_FILENAMES = {"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "Cargo.lock", "go.sum"}


def utc_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(raw):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def env_flag(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def split_csv_env(name):
    value = os.getenv(name, "")
    if not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def append_jsonl(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def run_command(command, timeout=10):
    return subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def is_git_repo():
    return run_command(["git", "rev-parse", "--is-inside-work-tree"]).returncode == 0


def current_prompt_text(raw):
    data = load_json(raw)
    if isinstance(data, dict):
        for key in ("userMessage", "prompt", "message", "text"):
            value = data.get(key)
            if value:
                return str(value)
    return raw


def tool_invocation(raw):
    data = load_json(raw)
    tool_name = ""
    tool_input = ""
    if isinstance(data, dict):
        tool_name = data.get("tool_name") or data.get("toolName") or ""
        raw_input = data.get("tool_input")
        if raw_input is None:
            raw_input = data.get("toolInput")
        if isinstance(raw_input, str):
            tool_input = raw_input
        elif raw_input is not None:
            tool_input = json.dumps(raw_input, ensure_ascii=False, sort_keys=True)
    return str(tool_name), str(tool_input)


def session_logger_start():
    if env_flag("SKIP_LOGGING"):
        return 0
    append_jsonl(COPILOT_LOG_DIR / "session.log", {"timestamp": utc_timestamp(), "event": "sessionStart", "cwd": str(ROOT)})
    print("Session logged")
    return 0


def session_logger_prompt():
    if env_flag("SKIP_LOGGING"):
        return 0
    append_jsonl(COPILOT_LOG_DIR / "prompts.log", {"timestamp": utc_timestamp(), "event": "userPromptSubmitted", "level": os.getenv("LOG_LEVEL", "INFO")})
    return 0


def session_logger_end():
    if env_flag("SKIP_LOGGING"):
        return 0
    append_jsonl(COPILOT_LOG_DIR / "session.log", {"timestamp": utc_timestamp(), "event": "sessionEnd"})
    print("Session end logged")
    return 0


def governance_session_start():
    if env_flag("SKIP_GOVERNANCE_AUDIT"):
        return 0
    level = os.getenv("GOVERNANCE_LEVEL", "standard")
    append_jsonl(COPILOT_LOG_DIR / "governance" / "audit.log", {"timestamp": utc_timestamp(), "event": "session_start", "governance_level": level, "cwd": str(ROOT)})
    print(f"Governance audit active (level: {level})")
    return 0


def governance_prompt(raw):
    if env_flag("SKIP_GOVERNANCE_AUDIT"):
        return 0
    prompt = current_prompt_text(raw)
    level = os.getenv("GOVERNANCE_LEVEL", "standard")
    block = env_flag("BLOCK_ON_THREAT")
    threats = []
    for pattern, category, severity, description in GOVERNANCE_PATTERNS:
        match = re.search(pattern, prompt, re.IGNORECASE | re.MULTILINE)
        if match:
            threats.append({"category": category, "severity": severity, "description": description, "evidence": match.group(0)[:120]})
    log_path = COPILOT_LOG_DIR / "governance" / "audit.log"
    if threats:
        max_severity = max(item["severity"] for item in threats)
        append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "threat_detected", "governance_level": level, "threat_count": len(threats), "max_severity": max_severity, "threats": threats})
        print(f"Governance: {len(threats)} threat signal(s) detected (max severity: {max_severity})")
        for item in threats:
            print(f"  - [{item['category']}] {item['description']} (severity: {item['severity']})")
        if block or level in {"strict", "locked"}:
            print("Prompt blocked by governance policy")
            return 1
        return 0
    append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "prompt_scanned", "governance_level": level, "status": "clean"})
    return 0


def governance_session_end():
    if env_flag("SKIP_GOVERNANCE_AUDIT"):
        return 0
    log_path = COPILOT_LOG_DIR / "governance" / "audit.log"
    total_events = 0
    threats_detected = 0
    if log_path.exists():
        try:
            with log_path.open("r", encoding="utf-8") as handle:
                lines = [json.loads(line) for line in handle if line.strip()]
            session_start_index = 0
            for index, item in enumerate(lines):
                if item.get("event") == "session_start":
                    session_start_index = index
            session_lines = lines[session_start_index:]
            total_events = len(session_lines)
            threats_detected = sum(1 for item in session_lines if item.get("event") == "threat_detected")
        except Exception:
            total_events = 0
            threats_detected = 0
    append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "session_end", "total_events": total_events, "threats_detected": threats_detected})
    if threats_detected:
        print(f"Session ended: {threats_detected} threat(s) detected in {total_events} events")
    else:
        print(f"Session ended: {total_events} events, no threats")
    return 0


def tool_guardian(raw):
    if env_flag("SKIP_TOOL_GUARD"):
        return 0
    mode = os.getenv("GUARD_MODE", "block")
    allowlist = split_csv_env("TOOL_GUARD_ALLOWLIST")
    tool_name, tool_input = tool_invocation(raw)
    combined = f"{tool_name} {tool_input}".strip()
    log_path = TOOL_GUARD_LOG_DIR / "guard.log"
    if allowlist and any(item in combined for item in allowlist):
        append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "guard_skipped", "reason": "allowlisted", "tool": tool_name})
        return 0
    threats = []
    for category, severity, pattern, suggestion in TOOL_GUARD_PATTERNS:
        match = re.search(pattern, combined, re.IGNORECASE)
        if match:
            threats.append({"category": category, "severity": severity, "match": match.group(0), "suggestion": suggestion})
    if not threats:
        append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "guard_passed", "mode": mode, "tool": tool_name})
        return 0
    print("")
    print(f"Tool Guardian: {len(threats)} threat(s) detected in '{tool_name}' invocation")
    print("")
    print(f"  {'CATEGORY':24} {'SEVERITY':10} {'MATCH':40} SUGGESTION")
    print(f"  {'--------':24} {'--------':10} {'-----':40} ----------")
    for item in threats:
        display_match = item["match"]
        if len(display_match) > 38:
            display_match = display_match[:35] + "..."
        print(f"  {item['category'][:24]:24} {item['severity'][:10]:10} {display_match[:40]:40} {item['suggestion']}")
    append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "threats_detected", "mode": mode, "tool": tool_name, "threat_count": len(threats), "threats": threats})
    print("")
    if mode == "block":
        print("Operation blocked: resolve the threats above or adjust TOOL_GUARD_ALLOWLIST.")
        return 1
    print("Threats logged in warn mode. Set GUARD_MODE=block to prevent dangerous operations.")
    return 0


def git_stdout(args, timeout=15):
    result = run_command(["git", *args], timeout=timeout)
    return result.stdout if result.returncode == 0 else ""


def unique_paths(items):
    seen = set()
    output = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            output.append(item)
    return output


def is_text_file(path):
    if path.name in TEXT_FILENAMES or path.suffix.lower() in TEXT_SUFFIXES:
        return True
    try:
        with path.open("rb") as handle:
            chunk = handle.read(8192)
        return b"\x00" not in chunk
    except OSError:
        return False


def redact_secret(value):
    return "[REDACTED]" if len(value) <= 12 else f"{value[:4]}...{value[-4:]}"


def secrets_scan():
    if env_flag("SKIP_SECRETS_SCAN"):
        print("Secrets scan skipped (SKIP_SECRETS_SCAN=true)")
        return 0
    if not is_git_repo():
        print("Not in a git repository, skipping secrets scan")
        return 0
    mode = os.getenv("SCAN_MODE", "warn")
    scope = os.getenv("SCAN_SCOPE", "diff")
    allowlist = split_csv_env("SECRETS_ALLOWLIST")
    log_path = Path(os.getenv("SECRETS_LOG_DIR", str(COPILOT_LOG_DIR / "secrets"))) / "scan.log"
    files = []
    if scope == "staged":
        files.extend(git_stdout(["diff", "--cached", "--name-only", "--diff-filter=ACMR"]).splitlines())
    else:
        diff_output = git_stdout(["diff", "--name-only", "--diff-filter=ACMR", "HEAD"])
        if not diff_output:
            diff_output = git_stdout(["diff", "--name-only", "--diff-filter=ACMR"])
        files.extend(diff_output.splitlines())
        files.extend(git_stdout(["ls-files", "--others", "--exclude-standard"]).splitlines())
    files = unique_paths(files)
    if not files:
        print("No modified files to scan")
        append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "scan_complete", "mode": mode, "scope": scope, "status": "clean", "files_scanned": 0})
        return 0
    print(f"Scanning {len(files)} modified file(s) for secrets...")
    findings = []
    placeholder_re = re.compile(r"(example|placeholder|your[_-]|xxx|changeme|todo|fixme|replace[_-]?me|dummy|fake|test[_-]?key|sample)", re.IGNORECASE)
    for rel_path in files:
        path = ROOT / rel_path
        if not path.exists():
            continue
        if rel_path in SKIP_SECRET_FILENAMES or path.suffix.lower() == ".lock":
            continue
        if not is_text_file(path):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            for pattern_name, severity, pattern in SECRETS_PATTERNS:
                for match in re.finditer(pattern, line):
                    matched_value = match.group(0)
                    if pattern_name == "INTERNAL_IP_PORT":
                        inner = re.search(r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+:[0-9]+", matched_value)
                        if not inner:
                            continue
                        matched_value = inner.group(0)
                    if placeholder_re.search(matched_value):
                        continue
                    if allowlist and any(item in matched_value for item in allowlist):
                        continue
                    findings.append({"file": rel_path, "line": line_number, "pattern": pattern_name, "severity": severity, "match": redact_secret(matched_value)})
    if findings:
        print("")
        print(f"Found {len(findings)} potential secret(s) in modified files:")
        print("")
        print(f"  {'FILE':40} {'LINE':6} {'PATTERN':28} SEVERITY")
        print(f"  {'----':40} {'----':6} {'-------':28} --------")
        for finding in findings:
            print(f"  {finding['file'][:40]:40} {str(finding['line'])[:6]:6} {finding['pattern'][:28]:28} {finding['severity']}")
        print("")
        append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "secrets_found", "mode": mode, "scope": scope, "files_scanned": len(files), "finding_count": len(findings), "findings": findings})
        if mode == "block":
            print("Session blocked: resolve the findings above before committing.")
            return 1
        print("Review the findings above. Set SCAN_MODE=block to prevent commits with secrets.")
        return 0
    print(f"No secrets detected in {len(files)} scanned file(s)")
    append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "scan_complete", "mode": mode, "scope": scope, "status": "clean", "files_scanned": len(files)})
    return 0


def diff_lines_for_file(rel_path):
    output = git_stdout(["diff", "HEAD", "--", rel_path])
    if not output:
        output = git_stdout(["diff", "--", rel_path])
    return output.splitlines()


def new_dependencies():
    deps = []
    ignored_package_json = {"name", "version", "description", "main", "scripts", "dependencies", "devDependencies", "peerDependencies", "optionalDependencies"}
    for line in diff_lines_for_file("package.json"):
        if line.startswith("+") and not line.startswith("+++"):
            match = re.match(r'^\+\s*"([^"]+)"\s*:\s*"[^"]+"', line)
            if match and match.group(1) not in ignored_package_json:
                deps.append(("npm", match.group(1)))
    for line in diff_lines_for_file("requirements.txt"):
        if line.startswith("+") and not line.startswith("+++"):
            clean = line[1:].strip()
            if clean and not clean.startswith("#"):
                pkg = re.split(r"[><=!~]", clean, maxsplit=1)[0].strip()
                if pkg:
                    deps.append(("pip", pkg))
    for line in diff_lines_for_file("pyproject.toml"):
        if line.startswith("+") and not line.startswith("+++"):
            match = re.match(r'^\+\s*"([A-Za-z0-9_-]+)', line)
            if match:
                deps.append(("pip", match.group(1)))
    for line in diff_lines_for_file("go.mod"):
        if line.startswith("+") and not line.startswith("+++"):
            match = re.match(r"^\+\s*([a-zA-Z0-9._/-]*\.[a-zA-Z0-9._/-]*)\s", line)
            if match and match.group(1) not in {"module", "go", "require"}:
                deps.append(("go", match.group(1)))
    for line in diff_lines_for_file("Gemfile"):
        if line.startswith("+") and not line.startswith("+++"):
            match = re.match(r"^\+\s*gem\s*['\"`]([^'\"`]+)", line)
            if match:
                deps.append(("ruby", match.group(1)))
    ignored_cargo = {"name", "version", "edition", "authors", "description", "license", "repository", "rust-version"}
    for line in diff_lines_for_file("Cargo.toml"):
        if line.startswith("+") and not line.startswith("+++"):
            match = re.match(r"^\+\s*([a-zA-Z0-9_-]+)\s*=", line)
            if match and match.group(1) not in ignored_cargo:
                deps.append(("rust", match.group(1)))
    return unique_paths([f"{ecosystem}:{package}" for ecosystem, package in deps])


def lookup_npm_license(package):
    package_path = ROOT / "node_modules" / package / "package.json"
    if package_path.exists():
        try:
            data = json.loads(package_path.read_text(encoding="utf-8"))
            if data.get("license"):
                return str(data["license"])
        except Exception:
            pass
    try:
        result = run_command(["npm", "view", package, "license"], timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip().splitlines()[0]
    except Exception:
        pass
    return "UNKNOWN"


def lookup_pip_license(package):
    commands = [[sys.executable, "-m", "pip", "show", package], ["pip", "show", package], ["pip3", "show", package]]
    for command in commands:
        try:
            result = run_command(command, timeout=5)
        except Exception:
            continue
        if result.returncode != 0:
            continue
        for line in result.stdout.splitlines():
            if line.lower().startswith("license:"):
                value = line.split(":", 1)[1].strip()
                return value or "UNKNOWN"
    return "UNKNOWN"


def lookup_go_license(package):
    roots = []
    if os.getenv("GOPATH"):
        roots.append(Path(os.getenv("GOPATH")) / "pkg" / "mod")
    roots.append(Path.home() / "go" / "pkg" / "mod")
    package_key = package.replace("/", os.sep)
    for root in roots:
        if not root.exists():
            continue
        for match in list(root.glob(f"**/{package_key}@*"))[:3]:
            for license_file in match.glob("LICENSE*"):
                try:
                    text = license_file.read_text(encoding="utf-8", errors="ignore").upper()
                except OSError:
                    continue
                if "GNU AFFERO GENERAL PUBLIC" in text:
                    return "AGPL-3.0"
                if "GNU LESSER GENERAL PUBLIC" in text:
                    return "LGPL"
                if "GNU GENERAL PUBLIC LICENSE" in text:
                    if "VERSION 3" in text:
                        return "GPL-3.0"
                    if "VERSION 2" in text:
                        return "GPL-2.0"
                    return "GPL"
                if "MIT LICENSE" in text:
                    return "MIT"
                if "APACHE LICENSE" in text:
                    return "Apache-2.0"
                if "BSD" in text:
                    return "BSD"
    return "UNKNOWN"


def lookup_ruby_license(package):
    try:
        result = run_command(["gem", "spec", package, "license"], timeout=5)
    except Exception:
        return "UNKNOWN"
    if result.returncode != 0:
        return "UNKNOWN"
    lines = [line.strip().lstrip("- ").strip() for line in result.stdout.splitlines()]
    lines = [line for line in lines if line and line not in {"---", "..."}]
    return lines[0] if lines else "UNKNOWN"


def lookup_rust_license(package):
    try:
        result = run_command(["cargo", "metadata", "--format-version", "1"], timeout=5)
    except Exception:
        return "UNKNOWN"
    if result.returncode != 0:
        return "UNKNOWN"
    try:
        data = json.loads(result.stdout)
        for item in data.get("packages", []):
            if item.get("name") == package:
                return item.get("license") or "UNKNOWN"
    except Exception:
        return "UNKNOWN"
    return "UNKNOWN"


def lookup_license(ecosystem, package):
    if ecosystem == "npm":
        return lookup_npm_license(package)
    if ecosystem == "pip":
        return lookup_pip_license(package)
    if ecosystem == "go":
        return lookup_go_license(package)
    if ecosystem == "ruby":
        return lookup_ruby_license(package)
    if ecosystem == "rust":
        return lookup_rust_license(package)
    return "UNKNOWN"


def license_is_blocked(license_name, blocked_list):
    license_lower = license_name.lower()
    return any(item.lower() in license_lower for item in blocked_list)


def dependency_license_check():
    if env_flag("SKIP_LICENSE_CHECK"):
        print("License check skipped (SKIP_LICENSE_CHECK=true)")
        return 0
    if not is_git_repo():
        print("Not in a git repository, skipping license check")
        return 0
    mode = os.getenv("LICENSE_MODE", "warn")
    blocked = split_csv_env("BLOCKED_LICENSES") or DEFAULT_BLOCKED_LICENSES
    allowlist = split_csv_env("LICENSE_ALLOWLIST")
    log_path = Path(os.getenv("LICENSE_LOG_DIR", str(COPILOT_LOG_DIR / "license-checker"))) / "check.log"
    deps = new_dependencies()
    if not deps:
        print("No new dependencies detected")
        append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "license_check_complete", "mode": mode, "status": "clean", "dependencies_checked": 0})
        return 0
    print(f"Checking licenses for {len(deps)} new dependency(ies)...")
    print("")
    print(f"  {'PACKAGE':30} {'ECOSYSTEM':12} {'LICENSE':30} STATUS")
    print(f"  {'-------':30} {'---------':12} {'-------':30} ------")
    results = []
    violations = []
    for dep in deps:
        ecosystem, package = dep.split(":", 1)
        license_name = lookup_license(ecosystem, package)
        status = "OK"
        if package in allowlist:
            status = "ALLOWLISTED"
        elif license_is_blocked(license_name, blocked):
            status = "BLOCKED"
            violations.append({"package": package, "ecosystem": ecosystem, "license": license_name, "status": status})
        results.append((package, ecosystem, license_name, status))
        print(f"  {package[:30]:30} {ecosystem[:12]:12} {license_name[:30]:30} {status}")
    print("")
    append_jsonl(log_path, {"timestamp": utc_timestamp(), "event": "license_check_complete", "mode": mode, "dependencies_checked": len(results), "violation_count": len(violations), "violations": violations})
    if violations:
        print(f"Found {len(violations)} license violation(s):")
        print("")
        for item in violations:
            print(f"  - {item['package']} ({item['ecosystem']}): {item['license']}")
        print("")
        if mode == "block":
            print("Session blocked: resolve license violations above before committing.")
            return 1
        print("Review the violations above. Set LICENSE_MODE=block to prevent commits with license issues.")
        return 0
    print(f"All {len(results)} dependencies have compliant licenses")
    return 0


def auto_commit():
    if env_flag("SKIP_AUTO_COMMIT", default=False):
        print("Auto-commit skipped (SKIP_AUTO_COMMIT=true)")
        return 0
    if not is_git_repo():
        print("Not in a git repository")
        return 0
    approved_by_env = env_flag("AUTO_COMMIT_APPROVED", default=False)
    approved_by_file = AUTO_COMMIT_APPROVAL_FILE.exists()
    if not approved_by_env and not approved_by_file:
        print(f"Auto-commit waiting for approval ({AUTO_COMMIT_APPROVAL_FILE})")
        return 0
    status = run_command(["git", "status", "--porcelain"])
    if status.returncode != 0 or not status.stdout.strip():
        print("No changes to commit")
        return 0
    if approved_by_file:
        try:
            AUTO_COMMIT_APPROVAL_FILE.unlink()
        except OSError:
            pass
    print("Auto-committing changes from Copilot session...")
    if run_command(["git", "add", "-A"]).returncode != 0:
        print("Commit staging failed")
        return 1
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit = run_command(["git", "commit", "-m", f"auto-commit: {timestamp}", "--no-verify"], timeout=20)
    if commit.returncode != 0:
        print("Commit failed")
        return 0
    print("Changes committed locally. Auto-push is disabled.")
    return 0


def approve_auto_commit():
    AUTO_COMMIT_APPROVAL_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUTO_COMMIT_APPROVAL_FILE.write_text(
        json.dumps({"approved_at": utc_timestamp()}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Auto-commit approved for the next local commit: {AUTO_COMMIT_APPROVAL_FILE}")
    return 0


def revoke_auto_commit():
    if AUTO_COMMIT_APPROVAL_FILE.exists():
        try:
            AUTO_COMMIT_APPROVAL_FILE.unlink()
        except OSError as exc:
            print(f"Failed to revoke auto-commit approval: {exc}", file=sys.stderr)
            return 1
        print("Auto-commit approval cleared.")
        return 0
    print("No auto-commit approval file to clear.")
    return 0


def prompt_pipeline(raw):
    for hook in (session_logger_start, governance_session_start, session_logger_prompt):
        code = hook()
        if code:
            return code
    return governance_prompt(raw)


def stop_pipeline():
    for hook in (session_logger_end, governance_session_end, secrets_scan, dependency_license_check, auto_commit):
        code = hook()
        if code:
            return code
    return 0


def main():
    valid_modes = {
        "pretool",
        "prompt",
        "stop",
        "work-start",
        "work-end",
        "guard-command",
        "approve-autocommit",
        "revoke-autocommit",
    }
    if len(sys.argv) != 2 or sys.argv[1] not in valid_modes:
        print(
            "Usage: python .claude/hooks/awesome_copilot_bridge.py "
            "[pretool|prompt|stop|work-start|work-end|guard-command|approve-autocommit|revoke-autocommit]",
            file=sys.stderr,
        )
        sys.exit(2)
    raw = sys.stdin.read()
    mode = sys.argv[1]
    try:
        if mode in {"pretool", "guard-command"}:
            code = tool_guardian(raw)
        elif mode in {"prompt", "work-start"}:
            code = prompt_pipeline(raw)
        elif mode == "approve-autocommit":
            code = approve_auto_commit()
        elif mode == "revoke-autocommit":
            code = revoke_auto_commit()
        else:
            code = stop_pipeline()
    except Exception as exc:
        print(f"awesome-copilot bridge failed: {exc}", file=sys.stderr)
        code = 1
    sys.exit(code)


if __name__ == "__main__":
    main()
