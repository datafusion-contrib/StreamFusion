# The published all-in-one launcher with the released 4.1.4.1 FE/BE binaries.
# 4.1.3's Arrow load planner cannot resolve the hidden delete-sign column.
FROM apache/doris:fe-4.1.4.1@sha256:a9da001c8afa6d07b9b3de532a0b65566c3bc134f0beefbedfce7fad006eb843 AS frontend
FROM apache/doris:be-4.1.4.1@sha256:0b2827e2fcc0fc9b007502bda2a89e00d97f5696ca47c89f95cdff8e488da249 AS backend
FROM apache/doris:all-in-one-4.1.3@sha256:82a5cabc7900ebcd3d080412d636c0cebd6602799fffe2605c652b2ea7d42609
RUN rm -rf /opt/apache-doris/fe /opt/apache-doris/be
COPY --from=frontend /opt/apache-doris/fe/ /opt/apache-doris/fe/
COPY --from=backend /opt/apache-doris/be/ /opt/apache-doris/be/
