const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const ROOT = path.resolve(__dirname, "..");
const LOCAL_AGENT_DIR = path.join(ROOT, ".claude", "agents");
const REF_AGENT_DIR = path.join(ROOT, "_ref_claude-agents");
const MCP_CONFIG_FILE = path.join(ROOT, ".mcp.json");
const SERVER_FILE = path.join(ROOT, "server.js");
const CLAUDE_SETTINGS_FILE = path.join(ROOT, ".claude", "settings.json");
const TOOL_GUARDIAN_HOOKS_FILE = path.join(
  ROOT,
  ".github",
  "hooks",
  "tool-guardian",
  "hooks.json",
);

function readText(filePath) {
  return fs.readFileSync(filePath, "utf8");
}

function sha256(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

function collectMarkdownFiles(rootDir) {
  const results = [];
  if (!fs.existsSync(rootDir)) {
    return results;
  }
  for (const entry of fs.readdirSync(rootDir, { withFileTypes: true })) {
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      results.push(...collectMarkdownFiles(fullPath));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".md")) {
      results.push(fullPath);
    }
  }
  return results;
}

function relativeToRoot(filePath) {
  return path.relative(ROOT, filePath).replace(/\\/g, "/");
}

function findRefAgentMatches() {
  const localAgents = collectMarkdownFiles(LOCAL_AGENT_DIR);
  const refAgents = collectMarkdownFiles(REF_AGENT_DIR);
  const refByName = new Map();

  for (const filePath of refAgents) {
    const name = path.basename(filePath);
    if (!refByName.has(name)) {
      refByName.set(name, []);
    }
    refByName.get(name).push(filePath);
  }

  const rows = [];
  for (const localPath of localAgents) {
    const fileName = path.basename(localPath);
    const refMatches = refByName.get(fileName) || [];
    if (refMatches.length !== 1) {
      rows.push({
        fileName,
        localPath,
        status: refMatches.length === 0 ? "local_only" : "ambiguous_ref",
        refPath: refMatches.map(relativeToRoot).join(", "),
      });
      continue;
    }
    rows.push({
      fileName,
      localPath,
      refPath: refMatches[0],
      status: sha256(localPath) === sha256(refMatches[0]) ? "matched" : "drifted",
    });
  }

  return rows.sort((left, right) => left.fileName.localeCompare(right.fileName, "en"));
}

function inspectMcpReference() {
  const issues = [];
  if (!fs.existsSync(MCP_CONFIG_FILE)) {
    issues.push("`.mcp.json` is missing.");
    return issues;
  }

  let parsedConfig = null;
  try {
    parsedConfig = JSON.parse(readText(MCP_CONFIG_FILE));
  } catch (error) {
    issues.push(`.mcp.json is not valid JSON: ${error.message}`);
    return issues;
  }

  const configuredCwd = parsedConfig?.mcpServers?.["real-estate"]?.cwd || "";
  if (configuredCwd !== "_ref_real-estate-mcp") {
    issues.push(`.mcp.json real-estate cwd is '${configuredCwd}', expected '_ref_real-estate-mcp'.`);
  }

  const hardcodedKey = parsedConfig?.mcpServers?.["real-estate"]?.env?.DATA_GO_KR_API_KEY;
  if (typeof hardcodedKey === "string" && hardcodedKey.trim()) {
    issues.push(".mcp.json contains a non-empty DATA_GO_KR_API_KEY. Prefer loading secrets from the local environment.");
  }

  const serverText = readText(SERVER_FILE);
  if (!serverText.includes('const MCP_SERVER_CWD = path.join(__dirname, "_ref_real-estate-mcp");')) {
    issues.push("server.js MCP_SERVER_CWD no longer points clearly to `_ref_real-estate-mcp`.");
  }

  return issues;
}

function inspectWorkspaceRisks() {
  const issues = [];

  if (!fs.existsSync(path.join(ROOT, ".git"))) {
    issues.push(
      "Workspace is not a git repository. Stop-hook secrets/license scans and git-based review helpers will be skipped.",
    );
  }

  if (fs.existsSync(TOOL_GUARDIAN_HOOKS_FILE)) {
    try {
      const parsedHooks = JSON.parse(readText(TOOL_GUARDIAN_HOOKS_FILE));
      const hookCommand =
        parsedHooks?.hooks?.preToolUse?.[0]?.bash ||
        parsedHooks?.hooks?.PreToolUse?.[0]?.bash ||
        "";
      if (hookCommand) {
        const resolvedHookPath = path.join(ROOT, hookCommand.replaceAll("/", path.sep));
        if (!fs.existsSync(resolvedHookPath)) {
          issues.push(
            `.github tool-guardian hook points to '${hookCommand}', but that file does not exist from the repo root.`,
          );
        }
      }
    } catch (error) {
      issues.push(`tool-guardian hooks.json is not valid JSON: ${error.message}`);
    }
  }

  if (fs.existsSync(CLAUDE_SETTINGS_FILE) && !fs.existsSync(path.join(ROOT, ".git"))) {
    try {
      const parsedSettings = JSON.parse(readText(CLAUDE_SETTINGS_FILE));
      const stopHooks = parsedSettings?.hooks?.Stop || [];
      const suggestsPrReviewer = JSON.stringify(stopHooks).includes("pr-reviewer");
      if (suggestsPrReviewer) {
        issues.push(
          "Claude Stop hook still suggests `pr-reviewer`, but the current workspace has no git metadata for branch/diff-based review.",
        );
      }
    } catch (error) {
      issues.push(`.claude/settings.json is not valid JSON: ${error.message}`);
    }
  }

  return issues;
}

function main() {
  const refRows = findRefAgentMatches();
  const configIssues = inspectMcpReference();
  const workspaceIssues = inspectWorkspaceRisks();
  const drifted = refRows.filter((row) => row.status === "drifted");
  const ambiguous = refRows.filter((row) => row.status === "ambiguous_ref");
  const localOnly = refRows.filter((row) => row.status === "local_only");
  const matched = refRows.filter((row) => row.status === "matched");

  console.log("Ref agent review");
  console.log(`- matched copies: ${matched.length}`);
  console.log(`- drifted copies: ${drifted.length}`);
  console.log(`- local-only agents: ${localOnly.length}`);
  console.log(`- ambiguous ref matches: ${ambiguous.length}`);

  if (matched.length) {
    console.log("\nMatched");
    for (const row of matched) {
      console.log(`- ${row.fileName}: ${relativeToRoot(row.refPath)}`);
    }
  }

  if (localOnly.length) {
    console.log("\nLocal-only");
    for (const row of localOnly) {
      console.log(`- ${row.fileName}`);
    }
  }

  if (drifted.length) {
    console.log("\nDrifted");
    for (const row of drifted) {
      console.log(`- ${row.fileName}: local=${relativeToRoot(row.localPath)} ref=${relativeToRoot(row.refPath)}`);
    }
  }

  if (ambiguous.length) {
    console.log("\nAmbiguous");
    for (const row of ambiguous) {
      console.log(`- ${row.fileName}: ${row.refPath}`);
    }
  }

  if (configIssues.length) {
    console.log("\nConfig warnings");
    for (const issue of configIssues) {
      console.log(`- ${issue}`);
    }
  }

  if (workspaceIssues.length) {
    console.log("\nWorkspace warnings");
    for (const issue of workspaceIssues) {
      console.log(`- ${issue}`);
    }
  }

  if (drifted.length || ambiguous.length) {
    process.exitCode = 1;
  }
}

main();
