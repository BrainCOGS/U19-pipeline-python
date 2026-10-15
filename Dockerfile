# Notebook/analysis image for the U19 pipeline.
# Installs the locked dependency set from uv.lock (datajoint <2.0, setuptools <82)
# plus the `analysis` extra so JupyterLab is available on port 8888.
# Python version matches .python-version; UV_PYTHON_DOWNLOADS=never makes the build
# fail loudly instead of downloading a second interpreter if they drift apart.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Keep the venv outside /src/U19-pipeline so a bind mount of the source tree
# (see docker-compose-local.yml) does not hide it.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /src/U19-pipeline

# Install third-party deps first so this layer is cached across source edits.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra analysis --no-install-project

# Then install the project itself (editable, as recorded in uv.lock).
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra analysis

EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--notebook-dir=/src/U19-pipeline/notebooks"]
