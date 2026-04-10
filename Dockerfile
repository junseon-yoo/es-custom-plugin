FROM docker.elastic.co/elasticsearch/elasticsearch:8.17.0

# Plugin is pre-extracted by the CI pipeline so the base image doesn't
# need unzip and the resulting layer is fully transparent.
# Expected layout of docker-context/plugin/:
#   es-bulk-id-export-1.0.0.jar
#   plugin-descriptor.properties
#   plugin-security.policy
USER root

COPY --chown=elasticsearch:elasticsearch docker-context/plugin/ \
     /usr/share/elasticsearch/plugins-bundled/es-bulk-id-export/

USER elasticsearch
