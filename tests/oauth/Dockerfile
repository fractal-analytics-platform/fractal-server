FROM dexidp/dex

COPY config.yaml /etc/dex/cfg/config.yaml

ENTRYPOINT ["dex", "serve", "/etc/dex/cfg/config.yaml"]
