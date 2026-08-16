#!/usr/bin/env bash
# Keeps the non-Claude agent config trees in sync with .claude/, which is the single source.
#
#   .claude/skills/   ->  .agents/skills/    (verbatim copy)
#   .claude/agents/   ->  .codex/agents/     (Markdown front matter converted to TOML)
#
# Run after editing anything under .claude/skills/ or .claude/agents/.
# Use --check to verify without writing; that form is the CI gate.

set -euo pipefail
cd "$(dirname "$0")/.."

CHECK=0
[[ "${1:-}" == "--check" ]] && CHECK=1

fail() { echo "out of sync: $1" >&2; echo "run scripts/sync-agent-config.sh" >&2; exit 1; }

# --- skills: verbatim mirror ------------------------------------------------
if [[ $CHECK -eq 1 ]]; then
    diff -r .claude/skills .agents/skills >/dev/null 2>&1 || fail ".agents/skills"
else
    rm -rf .agents/skills
    mkdir -p .agents
    cp -R .claude/skills .agents/skills
fi

# --- agents: markdown front matter -> toml ----------------------------------
render_toml() {
    local src="$1"
    local name description body
    name=$(awk -F': ' '/^name: /{print $2; exit}' "$src")
    description=$(awk -F': ' '/^description: /{sub(/^description: /,""); print; exit}' "$src")
    # Body is everything after the closing front matter delimiter.
    body=$(awk 'BEGIN{d=0} /^---$/{d++; next} d>=2{print}' "$src" | sed '/./,$!d')

    printf 'name = "%s"\n' "$name"
    printf 'description = "%s"\n' "$description"
    printf 'developer_instructions = """\n%s"""\n' "$body"
}

mkdir -p .codex/agents
for src in .claude/agents/*.md; do
    dest=".codex/agents/$(basename "${src%.md}").toml"
    if [[ $CHECK -eq 1 ]]; then
        diff <(render_toml "$src") "$dest" >/dev/null 2>&1 || fail "$dest"
    else
        render_toml "$src" > "$dest"
    fi
done

# Prune codex agents whose Claude source was deleted.
for dest in .codex/agents/*.toml; do
    src=".claude/agents/$(basename "${dest%.toml}").md"
    if [[ ! -f "$src" ]]; then
        if [[ $CHECK -eq 1 ]]; then fail "$dest has no source in .claude/agents"; else rm "$dest"; fi
    fi
done

[[ $CHECK -eq 1 ]] && echo "agent config in sync" || echo "agent config synced from .claude/"
