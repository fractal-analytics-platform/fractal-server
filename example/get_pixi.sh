mkdir -p /tmp/pixi
export PIXI_NO_PATH_UPDATE=1
export PIXI_VERSION=0.47.0
export PIXI_HOME=/tmp/pixi/$PIXI_VERSION
curl -fsSL https://pixi.sh/install.sh | sh
