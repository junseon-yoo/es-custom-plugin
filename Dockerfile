FROM docker.elastic.co/elasticsearch/elasticsearch:8.17.0

# Plugin is pre-extracted by the CI pipeline so the base image doesn't
# need unzip and the resulting layer is fully transparent.
# Expected layout of docker-context/plugin/:
#   es-bulk-id-export-1.0.0.jar
#   plugin-descriptor.properties
#   plugin-security.policy
USER root

# uid 1000 is the elasticsearch user in the official ES image.
# chown uses the numeric uid/gid so the result is deterministic.
COPY --chown=1000:0 docker-context/plugin/ \
     /usr/share/elasticsearch/plugins-bundled/es-bulk-id-export/

# Use numeric uid so Kubernetes runAsNonRoot admission can verify
# that the container user is non-root. Using the username "elasticsearch"
# fails with: "image has non-numeric user, cannot verify user is non-root".
USER 1000
