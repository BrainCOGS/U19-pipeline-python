#!/usr/bin/env bash
# Called only after release tests/builds pass, with a job-scoped contents-write token.
set -euo pipefail
: "${GH_REPO:?}" "${RELEASE_TAG:?}" "${RELEASE_COMMIT:?}" "${RELEASE_CHANNEL:?}" "${ARTIFACT_DIR:?}"
[[ "$RELEASE_TAG" =~ ^recording-transfer/v[0-9]+\.[0-9]+\.[0-9]+$ ]]
[[ "$RELEASE_COMMIT" =~ ^[a-f0-9]{40}$ ]]
[[ "$RELEASE_CHANNEL" == pilot || "$RELEASE_CHANNEL" == stable ]]

# --target alone does not verify an existing tag. Resolve lightweight/annotated
# tags via the commits endpoint and refuse source/artifact identity mismatches.
encoded_tag=${RELEASE_TAG//\//%2F}
existing=$(gh api "repos/$GH_REPO/commits/$encoded_tag" --jq .sha 2>/dev/null || true)
if [[ -n "$existing" ]]; then
    if [[ "$existing" != "$RELEASE_COMMIT" ]]; then
        echo 'Release tag already points at another commit; refusing publication.' >&2
        exit 1
    fi
else
    gh api "repos/$GH_REPO/git/refs" --method POST -f "ref=refs/tags/$RELEASE_TAG" -f "sha=$RELEASE_COMMIT" >/dev/null
fi

flags=(--draft --latest=false --verify-tag)
if [[ "$RELEASE_CHANNEL" == pilot ]]; then flags+=(--prerelease); fi
gh release create "$RELEASE_TAG" "$ARTIFACT_DIR"/* "${flags[@]}" \
    --title "U19 Recording Transfer ${RELEASE_TAG#recording-transfer/v}" \
    --notes 'Versioned Windows recording-transfer application. See the bundled DEPLOYMENT.md for installation and rollback.'
gh release edit "$RELEASE_TAG" --draft=false --latest=false
