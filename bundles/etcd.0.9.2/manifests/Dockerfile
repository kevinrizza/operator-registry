FROM scratch

LABEL operators.operatorframework.io.bundle.mediatype.v1=registry+v1
LABEL operators.operatorframework.io.bundle.manifests.v1=manifests/
LABEL operators.operatorframework.io.bundle.metadata.v1=metadata/
LABEL operators.operatorframework.io.bundle.package.v1=etcd
LABEL operators.operatorframework.io.bundle.channels.v1=alpha,stable
LABEL operators.operatorframework.io.bundle.channel.default.v1=alpha

ADD bundles/etcd.0.9.2/manifests/*.yaml /manifests/
ADD bundles/etcd.0.9.2/metadata/annotations.yaml /metadata/annotations.yaml
