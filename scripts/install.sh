#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

APP_REPO_DEFAULT="gaestu/SurfSifter"
DEFAULT_PREFIX="/usr/local"
DEFAULT_INSTALL_NAME="surfsifter"

DISTRO=""
PREFIX="${DEFAULT_PREFIX}"
BIN_SOURCE=""
INSTALL_NAME="${DEFAULT_INSTALL_NAME}"
WITH_RECOMMENDED_TOOLS=""
SKIP_TOOLS=false
VERIFY_ONLY=false
DRY_RUN=false
NON_INTERACTIVE=false
FORCE_INTERACTIVE=false
ASSUME_YES=false
RUN_VERIFY=true

FROM_RELEASE=false
RELEASE_VERSION="latest"
REPO="${APP_REPO_DEFAULT}"
RELEASE_ASSET=""
GITHUB_TOKEN="${SURFSIFTER_GITHUB_TOKEN:-${GITHUB_TOKEN:-}}"
SEEN_WITH_TOOLS=false
SEEN_SKIP_TOOLS=false

BIN_TARGET=""
RELEASE_TAG=""
RELEASE_URL=""
RELEASE_ASSET_NAME=""
BEFORE_VERSION=""
AFTER_VERSION=""

usage() {
  cat <<'EOF'
Usage: ./scripts/install.sh [options]

Installer for SurfSifter on Ubuntu/Fedora.
Interactive wizard runs by default on TTY when no explicit action flags are passed.

Options:
  --prefix <path>                 Install prefix (default: /usr/local)
  --bin-source <path>             Install local binary from path
  --from-release                  Download binary from GitHub Releases
  --release-version <tag|latest>  Release tag to install (default: latest)
  --repo <owner/name>             GitHub repo for releases (default: gaestu/SurfSifter)
  --release-asset <name>          Exact release asset name override
  --github-token <token>          GitHub token for private repos/releases (prefer env var)
  --install-name <name>           Target binary name in <prefix>/bin (default: surfsifter)
  --with-recommended-tools        Install recommended external tools
  --skip-tools                    Skip tool package installation
  --verify-only                   Only run verification checks
  --no-verify                     Skip verification after install
  --dry-run                       Print actions without changing system
  --interactive                   Force guided mode
  --non-interactive               Disable prompts (CI/automation)
  --yes                           Auto-confirm prompts in interactive mode
  --help                          Show help

Testing:
  SURFSIFTER_INSTALLER_DISTRO_OVERRIDE=ubuntu|fedora
  SURFSIFTER_GITHUB_TOKEN=<token> (preferred for private GitHub access)
EOF
}

log() { printf '[INFO] %s\n' "$*"; }
warn() { printf '[WARN] %s\n' "$*" >&2; }
err() { printf '[ERROR] %s\n' "$*" >&2; }

have_cmd() { command -v "$1" >/dev/null 2>&1; }

is_tty() {
  [[ -t 0 && -t 1 ]]
}

can_prompt() {
  [[ -t 1 && -r /dev/tty ]]
}

prompt_read() {
  local __var_name="$1"
  local __prompt="$2"
  local __value=""

  if can_prompt; then
    read -r -p "${__prompt}" __value < /dev/tty || true
  else
    read -r -p "${__prompt}" __value || true
  fi

  printf -v "${__var_name}" '%s' "${__value}"
}

prompt_secret_read() {
  local __var_name="$1"
  local __prompt="$2"
  local __value=""

  if can_prompt; then
    read -r -s -p "${__prompt}" __value < /dev/tty || true
    printf '\n' > /dev/tty
  else
    read -r -s -p "${__prompt}" __value || true
    printf '\n'
  fi

  printf -v "${__var_name}" '%s' "${__value}"
}

is_root() {
  [[ "${EUID}" -eq 0 ]]
}

confirm() {
  local prompt="$1"
  local default_no="${2:-false}"

  if $ASSUME_YES; then
    return 0
  fi

  local answer=""
  if $default_no; then
    prompt_read answer "${prompt} [y/N]: "
    [[ "${answer}" =~ ^[Yy]$ ]]
  else
    prompt_read answer "${prompt} [Y/n]: "
    [[ -z "${answer}" || "${answer}" =~ ^[Yy]$ ]]
  fi
}

run_cmd() {
  local -a cmd=("$@")
  if $DRY_RUN; then
    printf '[DRY-RUN] %q' "${cmd[0]}"
    local i
    for ((i = 1; i < ${#cmd[@]}; i++)); do
      printf ' %q' "${cmd[i]}"
    done
    printf '\n'
    return 0
  fi
  "${cmd[@]}"
}

run_priv() {
  if is_root; then
    run_cmd "$@"
  else
    run_cmd sudo "$@"
  fi
}

need_privileges() {
  if is_root; then
    return
  fi
  if ! have_cmd sudo; then
    err "This action requires root or sudo, but sudo is not available."
    exit 1
  fi
}

detect_distro() {
  local override="${SURFSIFTER_INSTALLER_DISTRO_OVERRIDE:-}"
  if [[ -n "${override}" ]]; then
    DISTRO="${override}"
    return
  fi

  if [[ ! -f /etc/os-release ]]; then
    err "Cannot detect distro: /etc/os-release not found."
    exit 1
  fi

  # shellcheck disable=SC1091
  source /etc/os-release
  DISTRO="${ID:-}"
  if [[ -z "${DISTRO}" && -n "${ID_LIKE:-}" ]]; then
    DISTRO="${ID_LIKE%% *}"
  fi
}

normalize_distro() {
  case "${DISTRO}" in
    ubuntu|debian) DISTRO="ubuntu" ;;
    fedora|rhel|centos) DISTRO="fedora" ;;
    *)
      err "Unsupported distro '${DISTRO}'. Supported: Ubuntu, Fedora."
      exit 1
      ;;
  esac
}

arch_token() {
  case "$(uname -m)" in
    x86_64|amd64) echo "linux-x86_64" ;;
    aarch64|arm64) echo "linux-arm64" ;;
    *)
      err "Unsupported architecture: $(uname -m)"
      exit 1
      ;;
  esac
}

ubuntu_runtime_packages() {
  echo "libcairo2 libpango-1.0-0 libgdk-pixbuf2.0-0 shared-mime-info libxcb-cursor0 libxcb-xinerama0 libxkbcommon0 libegl1 libgl1"
}

ubuntu_tool_packages() {
  echo "foremost scalpel libimage-exiftool-perl firejail ewf-tools sleuthkit"
}

fedora_runtime_packages() {
  echo "cairo pango gdk-pixbuf2 shared-mime-info qt6-qtbase libxcb libxkbcommon mesa-libEGL mesa-libGL"
}

fedora_tool_packages() {
  echo "foremost scalpel perl-Image-ExifTool firejail ewftools sleuthkit"
}

install_runtime_deps() {
  local pkgs=""
  case "${DISTRO}" in
    ubuntu) pkgs="$(ubuntu_runtime_packages)" ;;
    fedora) pkgs="$(fedora_runtime_packages)" ;;
  esac

  if [[ -z "${pkgs}" ]]; then
    return
  fi

  log "Installing runtime dependencies for ${DISTRO}."
  need_privileges
  if [[ "${DISTRO}" == "ubuntu" ]]; then
    run_priv apt-get update
    run_priv apt-get install -y ${pkgs}
  else
    run_priv dnf install -y ${pkgs}
  fi
}

install_recommended_tools() {
  local pkgs=""
  case "${DISTRO}" in
    ubuntu) pkgs="$(ubuntu_tool_packages)" ;;
    fedora) pkgs="$(fedora_tool_packages)" ;;
  esac

  if [[ -z "${pkgs}" ]]; then
    return
  fi

  log "Installing recommended tool packages for ${DISTRO}."
  need_privileges
  if [[ "${DISTRO}" == "ubuntu" ]]; then
    run_priv apt-get install -y ${pkgs}
  else
    run_priv dnf install -y ${pkgs}
  fi

  warn "bulk_extractor is not available in default ${DISTRO} repositories."
  warn "The app remains usable without it (URL fallback extractors stay available)."
}

auto_local_bin_source() {
  local candidate1="${PROJECT_ROOT}/dist/SurfSifter"
  local candidate2="${PROJECT_ROOT}/dist/surfsifter"
  if [[ -f "${candidate1}" ]]; then
    echo "${candidate1}"
    return
  fi
  if [[ -f "${candidate2}" ]]; then
    echo "${candidate2}"
    return
  fi
  echo ""
}

get_binary_version() {
  local bin="$1"
  if [[ ! -x "${bin}" ]]; then
    echo ""
    return
  fi
  local out
  out="$("${bin}" --version 2>/dev/null | head -n 1 || true)"
  echo "${out}"
}

parse_release_json() {
  local json_file="$1"
  local arch="$2"
  local asset_hint="$3"

  python3 - "${json_file}" "${arch}" "${asset_hint}" <<'PY'
import json
import re
import sys
from pathlib import Path

json_path = Path(sys.argv[1])
arch = sys.argv[2].lower()
asset_hint = sys.argv[3].strip().lower()

raw = json.loads(json_path.read_text(encoding="utf-8"))

# Handle both single release object and array of releases.
if isinstance(raw, list):
    if not raw:
        sys.exit(2)
    data = raw[0]
else:
    data = raw

tag = data.get("tag_name", "")
assets = data.get("assets", [])

bad_suffixes = (
    ".sha256",
    ".sha512",
    ".sig",
    ".asc",
    ".txt",
    ".md",
    ".json",
    ".sbom",
)
preferred_names = (
    "surfsifter",
)
arch_tokens = {
    "linux-x86_64": ["linux-x86_64", "x86_64", "amd64"],
    "linux-arm64": ["linux-arm64", "aarch64", "arm64"],
}
tokens = arch_tokens.get(arch, [arch])

def score(asset):
    name = asset.get("name", "")
    lower = name.lower()
    if not name:
        return -1
    if any(lower.endswith(s) for s in bad_suffixes):
        return -1
    s = 0
    if asset_hint:
        if lower == asset_hint:
            s += 1000
        elif asset_hint in lower:
            s += 350
    if "linux" in lower:
        s += 150
    if any(t in lower for t in tokens):
        s += 250
    if any(n in lower for n in preferred_names):
        s += 300
    if lower.endswith((".tar.gz", ".tgz", ".tar.xz", ".zip")):
        s += 100
    if re.search(r"(checksum|sha256|sha512|signature|sbom)", lower):
        s -= 300
    return s

ranked = sorted(((score(a), a) for a in assets), key=lambda x: x[0], reverse=True)
best = ranked[0][1] if ranked and ranked[0][0] > 0 else None
if best is None:
    sys.exit(2)

print(tag)
print(best.get("browser_download_url", ""))
print(best.get("name", ""))
PY
}

download_release_binary() {
  local arch
  arch="$(arch_token)"

  local api_url
  if [[ "${RELEASE_VERSION}" == "latest" ]]; then
    api_url="https://api.github.com/repos/${REPO}/releases/latest"
  else
    api_url="https://api.github.com/repos/${REPO}/releases/tags/${RELEASE_VERSION}"
  fi

  if $DRY_RUN; then
    log "Dry-run release source enabled (${REPO}, version=${RELEASE_VERSION}, arch=${arch})."
    printf '[DRY-RUN] curl -fsSL -H %q %q > %q\n' "Accept: application/vnd.github+json" "${api_url}" "${TMP_DIR}/release.json"
    RELEASE_TAG="${RELEASE_VERSION}"
    RELEASE_URL="https://github.com/${REPO}/releases"
    RELEASE_ASSET_NAME="<dry-run-asset>"
    BIN_SOURCE="<release-download:${RELEASE_VERSION}>"
    return
  fi

  local json_file="${TMP_DIR}/release.json"
  local api_fetched=false
  local tried_with_prompt=false
  local http_code=""

  while true; do
    if have_cmd curl; then
      local -a curl_args=(-sL -H "Accept: application/vnd.github+json" -o "${json_file}" -w "%{http_code}")
      if [[ -n "${GITHUB_TOKEN}" ]]; then
        curl_args+=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
      fi
      http_code="$(curl "${curl_args[@]}" "${api_url}" 2>/dev/null || true)"
    elif have_cmd wget; then
      local -a wget_args=(--server-response -qO "${json_file}" --header="Accept: application/vnd.github+json")
      if [[ -n "${GITHUB_TOKEN}" ]]; then
        wget_args+=(--header="Authorization: Bearer ${GITHUB_TOKEN}")
      fi
      local wget_stderr
      if wget_stderr="$(wget "${wget_args[@]}" "${api_url}" 2>&1)"; then
        http_code="200"
      else
        http_code="$(echo "${wget_stderr}" | grep -oP 'HTTP/[0-9.]+ \K[0-9]+' | tail -n 1)"
        http_code="${http_code:-000}"
      fi
    else
      err "Neither curl nor wget is available for release download."
      exit 1
    fi

    if [[ "${http_code}" == "200" ]]; then
      api_fetched=true
      break
    fi

    # 404 on /releases/latest can mean no *full* releases (pre-releases are excluded).
    # Fall back to the releases list endpoint which includes pre-releases.
    if [[ "${http_code}" == "404" ]]; then
      if [[ "${RELEASE_VERSION}" == "latest" && "${api_url}" == *"/releases/latest" ]]; then
        log "No full release found; checking for pre-releases."
        api_url="https://api.github.com/repos/${REPO}/releases?per_page=1"
        continue
      fi
      if [[ "${RELEASE_VERSION}" == "latest" ]]; then
        err "No releases (including pre-releases) found for ${REPO}."
        err "Use --bin-source to install from a local binary, or publish a release first."
      else
        err "Release '${RELEASE_VERSION}' not found for ${REPO}."
        err "Check that the tag exists at https://github.com/${REPO}/releases"
      fi
      exit 1
    fi

    # 401/403 — authentication issue, offer to prompt for a token.
    if [[ "${http_code}" == "401" || "${http_code}" == "403" ]]; then
      if $tried_with_prompt || [[ -n "${GITHUB_TOKEN}" && ! $tried_with_prompt ]]; then
        if [[ -n "${GITHUB_TOKEN}" ]]; then
          err "GitHub token was rejected (HTTP ${http_code}). Check token permissions."
        fi
        if $tried_with_prompt; then break; fi
      fi
      if [[ -z "${GITHUB_TOKEN}" ]] && can_prompt; then
        warn "GitHub API returned ${http_code} — authentication required."
        prompt_secret_read GITHUB_TOKEN "GitHub token (leave empty to abort): "
        if [[ -z "${GITHUB_TOKEN}" ]]; then
          break
        fi
        tried_with_prompt=true
        continue
      fi
      break
    fi

    # Any other error.
    warn "GitHub API returned HTTP ${http_code}."
    break
  done

  if ! $api_fetched; then
    err "Failed to fetch release metadata from ${api_url} (HTTP ${http_code})."
    if [[ "${http_code}" == "401" || "${http_code}" == "403" ]]; then
      err "For private repos, export SURFSIFTER_GITHUB_TOKEN (or use --github-token)."
    fi
    exit 1
  fi

  local parsed
  if ! parsed="$(parse_release_json "${json_file}" "${arch}" "${RELEASE_ASSET}")"; then
    err "Failed to resolve a matching release asset for ${arch} in ${REPO} (${RELEASE_VERSION})."
    exit 1
  fi

  RELEASE_TAG="$(echo "${parsed}" | sed -n '1p')"
  RELEASE_URL="$(echo "${parsed}" | sed -n '2p')"
  RELEASE_ASSET_NAME="$(echo "${parsed}" | sed -n '3p')"

  if [[ -z "${RELEASE_URL}" ]]; then
    err "Resolved release asset is missing download URL."
    exit 1
  fi

  local download_path="${TMP_DIR}/${RELEASE_ASSET_NAME}"
  log "Downloading release asset ${RELEASE_ASSET_NAME} (${RELEASE_TAG})."
  if have_cmd curl; then
    if [[ -n "${GITHUB_TOKEN}" ]]; then
      curl -fL -H "Authorization: Bearer ${GITHUB_TOKEN}" "${RELEASE_URL}" -o "${download_path}"
    else
      curl -fL "${RELEASE_URL}" -o "${download_path}"
    fi
  else
    if [[ -n "${GITHUB_TOKEN}" ]]; then
      wget -O "${download_path}" --header="Authorization: Bearer ${GITHUB_TOKEN}" "${RELEASE_URL}"
    else
      wget -O "${download_path}" "${RELEASE_URL}"
    fi
  fi

  case "${download_path}" in
    *.tar.gz|*.tgz)
      tar -xzf "${download_path}" -C "${TMP_DIR}"
      ;;
    *.tar.xz)
      tar -xJf "${download_path}" -C "${TMP_DIR}"
      ;;
    *.zip)
      if ! have_cmd unzip; then
        err "Asset is zip but unzip is not installed."
        exit 1
      fi
      unzip -q "${download_path}" -d "${TMP_DIR}/unzipped"
      ;;
    *)
      ;;
  esac

  local search_root="${TMP_DIR}"
  if [[ "${download_path}" == *.zip ]]; then
    search_root="${TMP_DIR}/unzipped"
  fi

  local found
  found="$(find "${search_root}" -type f \( -name "surfsifter" -o -name "SurfSifter" -o -name "surfsifter*" -o -name "SurfSifter*" \) | head -n 1 || true)"
  if [[ -z "${found}" && ! "${download_path}" =~ \.(tar\.gz|tgz|tar\.xz|zip)$ ]]; then
    found="${download_path}"
  fi
  if [[ -z "${found}" ]]; then
    err "Could not find installable binary in release asset ${RELEASE_ASSET_NAME}."
    exit 1
  fi

  BIN_SOURCE="${found}"
}

resolve_binary_source() {
  if [[ -n "${BIN_SOURCE}" ]]; then
    return
  fi

  if $FROM_RELEASE; then
    download_release_binary
    return
  fi

  local local_source=""
  local_source="$(auto_local_bin_source)"
  if [[ -n "${local_source}" ]]; then
    BIN_SOURCE="${local_source}"
    return
  fi

  err "No binary source found."
  err "Provide --bin-source, or use --from-release, or place binary in dist/SurfSifter or dist/surfsifter."
  exit 1
}

guided_source_selection() {
  local local_source=""
  local_source="$(auto_local_bin_source)"

  if $VERIFY_ONLY; then
    return
  fi

  if [[ -n "${BIN_SOURCE}" || $FROM_RELEASE == true ]]; then
    return
  fi

  if [[ -n "${local_source}" ]]; then
    printf '\nBinary source options:\n'
    printf '  1) Local dist binary (%s)\n' "${local_source}"
    printf '  2) Download latest GitHub Release\n'
    local choice=""
    prompt_read choice "Choose source [1/2] (default 1): "
    if [[ "${choice}" == "2" ]]; then
      FROM_RELEASE=true
      RELEASE_VERSION="latest"
    else
      BIN_SOURCE="${local_source}"
    fi
  else
    printf '\nNo local dist binary found.\n'
    printf 'Will use GitHub Releases by default.\n'
    FROM_RELEASE=true
    local rel=""
    prompt_read rel "Release version (latest or tag, default latest): "
    if [[ -n "${rel}" ]]; then
      RELEASE_VERSION="${rel}"
    fi
  fi
}

guided_prompts() {
  printf '\nSurfSifter installer\n'
  printf 'Detected distro: %s\n' "${DISTRO}"
  printf 'Default prefix: %s\n' "${PREFIX}"
  printf 'Press Enter to accept defaults.\n'

  guided_source_selection

  local answer=""
  prompt_read answer "Install prefix [${PREFIX}]: "
  if [[ -n "${answer}" ]]; then
    PREFIX="${answer}"
  fi

  local install_tools_default="Y"
  if [[ "${WITH_RECOMMENDED_TOOLS}" == "false" || "${SKIP_TOOLS}" == "true" ]]; then
    install_tools_default="N"
  fi
  if [[ -z "${WITH_RECOMMENDED_TOOLS}" ]]; then
    if [[ "${install_tools_default}" == "Y" ]]; then
      if confirm "Install recommended forensic tools?" false; then
        WITH_RECOMMENDED_TOOLS="true"
      else
        WITH_RECOMMENDED_TOOLS="false"
      fi
    else
      if confirm "Install recommended forensic tools?" true; then
        WITH_RECOMMENDED_TOOLS="true"
      else
        WITH_RECOMMENDED_TOOLS="false"
      fi
    fi
  fi

  if $RUN_VERIFY; then
    if ! confirm "Run verification after install?" false; then
      RUN_VERIFY=false
    fi
  fi

  if ! $DRY_RUN; then
    if confirm "Switch to dry-run (print commands only)?" true; then
      DRY_RUN=true
    fi
  fi

  printf '\nPlan summary:\n'
  printf '  distro: %s\n' "${DISTRO}"
  printf '  prefix: %s\n' "${PREFIX}"
  printf '  install name: %s\n' "${INSTALL_NAME}"
  printf '  source: %s\n' "${BIN_SOURCE:-release:${RELEASE_VERSION}}"
  printf '  install tools: %s\n' "${WITH_RECOMMENDED_TOOLS:-false}"
  printf '  verify after install: %s\n' "${RUN_VERIFY}"
  printf '  dry-run: %s\n' "${DRY_RUN}"

  if ! confirm "Proceed?" true; then
    err "Cancelled by user."
    exit 1
  fi
}

install_binary() {
  if $VERIFY_ONLY; then
    return
  fi

  resolve_binary_source
  BIN_TARGET="${PREFIX}/bin/${INSTALL_NAME}"

  if ! $DRY_RUN; then
    if [[ ! -f "${BIN_SOURCE}" ]]; then
      err "Binary source not found: ${BIN_SOURCE}"
      exit 1
    fi
  fi

  need_privileges
  run_priv mkdir -p "${PREFIX}/bin"

  if [[ -e "${BIN_TARGET}" && ! $DRY_RUN ]]; then
    BEFORE_VERSION="$(get_binary_version "${BIN_TARGET}")"
  fi

  local backup="${BIN_TARGET}.bak"
  local staging="${BIN_TARGET}.new"

  if [[ -e "${BIN_TARGET}" ]]; then
    run_priv cp -f "${BIN_TARGET}" "${backup}"
  fi

  if ! run_priv install -m 0755 "${BIN_SOURCE}" "${staging}"; then
    err "Failed to stage new binary."
    exit 1
  fi

  if ! run_priv mv -f "${staging}" "${BIN_TARGET}"; then
    err "Failed to replace existing binary."
    if [[ -e "${backup}" ]]; then
      run_priv mv -f "${backup}" "${BIN_TARGET}" || true
    fi
    exit 1
  fi

  # Keep backup only for same-run recovery; clean up once install succeeds.
  if [[ -e "${backup}" ]]; then
    run_priv rm -f "${backup}" || true
  fi

  if [[ -x "${BIN_TARGET}" && ! $DRY_RUN ]]; then
    AFTER_VERSION="$(get_binary_version "${BIN_TARGET}")"
  fi
}

verify_tools_with_python_registry() {
  if ! have_cmd python3; then
    return 1
  fi
  if [[ ! -f "${PROJECT_ROOT}/src/core/tool_registry.py" ]]; then
    return 1
  fi

  python3 - "${PROJECT_ROOT}" <<'PY'
import sys
from pathlib import Path

project_root = Path(sys.argv[1])
sys.path.insert(0, str(project_root / "src"))

try:
    from core.tool_registry import ToolRegistry
except Exception:
    sys.exit(2)

registry = ToolRegistry()
tools = registry.discover_all_tools()
for key in ("bulk_extractor", "foremost", "scalpel", "exiftool", "firejail", "ewfmount"):
    info = tools.get(key)
    if info is None:
        print(f"{key}: missing")
        continue
    details = str(info.path) if info.path else "n/a"
    print(f"{key}: {info.status} ({details})")
PY
}

verify_tools() {
  local missing=0
  printf '\nVerification:\n'

  if output="$(verify_tools_with_python_registry)"; then
    printf '%s\n' "${output}"
    local line
    while IFS= read -r line; do
      case "${line}" in
        *": missing"*|*": error"*)
          missing=$((missing + 1))
          ;;
      esac
    done <<< "${output}"
  else
    local tools=(bulk_extractor foremost scalpel exiftool firejail ewfmount)
    local t
    for t in "${tools[@]}"; do
      if have_cmd "${t}"; then
        printf '  %s: found (%s)\n' "${t}" "$(command -v "${t}")"
      else
        printf '  %s: missing\n' "${t}"
        missing=$((missing + 1))
      fi
    done
  fi

  local sk
  for sk in fls mmls icat; do
    if have_cmd "${sk}"; then
      printf '  sleuthkit:%s: found (%s)\n' "${sk}" "$(command -v "${sk}")"
    elif [[ -x "${PROJECT_ROOT}/vendor/sleuthkit/$(arch_token)/${sk}" ]]; then
      printf '  sleuthkit:%s: found (%s)\n' "${sk}" "${PROJECT_ROOT}/vendor/sleuthkit/$(arch_token)/${sk}"
    else
      printf '  sleuthkit:%s: missing\n' "${sk}"
      missing=$((missing + 1))
    fi
  done

  if [[ "${missing}" -gt 0 ]]; then
    warn "Verification found ${missing} missing tools."
  else
    log "Verification passed: all checked tools found."
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --prefix)
        PREFIX="${2:-}"
        shift 2
        ;;
      --bin-source)
        BIN_SOURCE="${2:-}"
        shift 2
        ;;
      --from-release)
        FROM_RELEASE=true
        shift
        ;;
      --release-version)
        RELEASE_VERSION="${2:-}"
        shift 2
        ;;
      --repo)
        REPO="${2:-}"
        shift 2
        ;;
      --release-asset)
        RELEASE_ASSET="${2:-}"
        shift 2
        ;;
      --github-token)
        GITHUB_TOKEN="${2:-}"
        shift 2
        ;;
      --install-name)
        INSTALL_NAME="${2:-}"
        shift 2
        ;;
      --with-recommended-tools)
        WITH_RECOMMENDED_TOOLS="true"
        SEEN_WITH_TOOLS=true
        shift
        ;;
      --skip-tools)
        SKIP_TOOLS=true
        WITH_RECOMMENDED_TOOLS="false"
        SEEN_SKIP_TOOLS=true
        shift
        ;;
      --verify-only)
        VERIFY_ONLY=true
        shift
        ;;
      --no-verify)
        RUN_VERIFY=false
        shift
        ;;
      --dry-run)
        DRY_RUN=true
        shift
        ;;
      --interactive)
        FORCE_INTERACTIVE=true
        shift
        ;;
      --non-interactive)
        NON_INTERACTIVE=true
        shift
        ;;
      --yes)
        ASSUME_YES=true
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        err "Unknown option: $1"
        usage
        exit 1
        ;;
    esac
  done
}

validate_args() {
  if [[ -z "${PREFIX}" ]]; then
    err "--prefix cannot be empty."
    exit 1
  fi
  if [[ -z "${INSTALL_NAME}" ]]; then
    err "--install-name cannot be empty."
    exit 1
  fi
  if [[ "${RELEASE_VERSION}" == "" ]]; then
    err "--release-version cannot be empty."
    exit 1
  fi
  if [[ "${REPO}" != */* ]]; then
    err "--repo must be in owner/name format."
    exit 1
  fi
  if [[ -n "${BIN_SOURCE}" && $FROM_RELEASE == true ]]; then
    err "--bin-source and --from-release are mutually exclusive."
    exit 1
  fi
  if $SEEN_SKIP_TOOLS && $SEEN_WITH_TOOLS; then
    err "--skip-tools and --with-recommended-tools are mutually exclusive."
    exit 1
  fi
  if $FORCE_INTERACTIVE && ! can_prompt; then
    err "--interactive requested, but no interactive TTY is available."
    err "Use --non-interactive for CI/automation, or run from a terminal."
    exit 1
  fi
}

should_run_interactive() {
  if $NON_INTERACTIVE; then
    return 1
  fi
  if $FORCE_INTERACTIVE; then
    return 0
  fi
  if ! can_prompt; then
    return 1
  fi
  if $VERIFY_ONLY; then
    return 0
  fi
  # If no explicit behavioral flags were given, run guided mode.
  if [[ -z "${BIN_SOURCE}" && "${FROM_RELEASE}" == "false" && -z "${WITH_RECOMMENDED_TOOLS}" && "${SKIP_TOOLS}" == "false" ]]; then
    return 0
  fi
  return 1
}

print_summary() {
  printf '\nInstall summary:\n'
  printf '  distro: %s\n' "${DISTRO}"
  printf '  prefix: %s\n' "${PREFIX}"
  printf '  target: %s\n' "${BIN_TARGET:-<none>}"
  if [[ -n "${RELEASE_TAG}" || -n "${RELEASE_URL}" ]]; then
    printf '  source: release %s (%s)\n' "${RELEASE_TAG:-${RELEASE_VERSION}}" "${RELEASE_ASSET_NAME:-${RELEASE_URL}}"
  elif [[ -n "${BIN_SOURCE}" ]]; then
    printf '  source: local %s\n' "${BIN_SOURCE}"
  fi
  printf '  tools requested: %s\n' "${WITH_RECOMMENDED_TOOLS:-false}"
  if [[ -n "${BEFORE_VERSION}" ]]; then
    printf '  previous version: %s\n' "${BEFORE_VERSION}"
  fi
  if [[ -n "${AFTER_VERSION}" ]]; then
    printf '  installed version: %s\n' "${AFTER_VERSION}"
  fi
  printf '\nNext checks:\n'
  if [[ -n "${BIN_TARGET}" ]]; then
    printf '  %q --version\n' "${BIN_TARGET}"
  fi
  printf '  command -v fls mmls icat\n'
}

main() {
  parse_args "$@"
  detect_distro
  normalize_distro
  validate_args

  if should_run_interactive; then
    guided_prompts
  fi

  if [[ -z "${WITH_RECOMMENDED_TOOLS}" ]]; then
    WITH_RECOMMENDED_TOOLS="false"
  fi

  if $VERIFY_ONLY; then
    verify_tools
    print_summary
    exit 0
  fi

  install_runtime_deps
  if [[ "${WITH_RECOMMENDED_TOOLS}" == "true" ]] && ! $SKIP_TOOLS; then
    install_recommended_tools
  fi
  install_binary

  if $RUN_VERIFY; then
    verify_tools
  fi
  print_summary
}

main "$@"
