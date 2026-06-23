ARG PG_MAJOR=15
FROM postgres:${PG_MAJOR}

ARG PG_MAJOR
ARG PACKAGE_DIR=build/pg15-package

COPY ${PACKAGE_DIR}/usr/lib/postgresql/${PG_MAJOR}/lib/grip_ext.so /usr/lib/postgresql/${PG_MAJOR}/lib/grip_ext.so
COPY ${PACKAGE_DIR}/usr/share/postgresql/${PG_MAJOR}/extension/grip_ext.control /usr/share/postgresql/${PG_MAJOR}/extension/grip_ext.control
COPY ${PACKAGE_DIR}/usr/share/postgresql/${PG_MAJOR}/extension/grip_ext--0.1.0.sql /usr/share/postgresql/${PG_MAJOR}/extension/grip_ext--0.1.0.sql
COPY docker/initdb/001-create-extension.sql /docker-entrypoint-initdb.d/001-create-extension.sql