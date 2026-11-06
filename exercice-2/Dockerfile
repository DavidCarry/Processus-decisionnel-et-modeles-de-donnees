# Apache Hop + pilote JDBC DuckDB. DuckDB lit la colonne « liasse » (de type map) des données financières,
# que les transforms Parquet de Hop ne savent pas découper, et compte les lignes des fichiers téléchargés.
# Base Java glibc : le moteur natif de DuckDB ne fonctionne pas sur l'image officielle de Hop (Alpine, musl).
FROM eclipse-temurin:21-jre-noble

COPY --from=apache/hop:2.19.0 /opt/hop /opt/hop
ADD --chmod=644 https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.5.6.0/duckdb_jdbc-1.5.6.0.jar /opt/hop/lib/jdbc/
COPY --chmod=755 lancer.sh /opt/hop/lancer.sh

ENTRYPOINT ["/opt/hop/lancer.sh"]
