package science.atlarge.graphalytics.duckdb.algorithms.lcc;

import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.duckdb.DuckdbConfiguration;
import science.atlarge.graphalytics.duckdb.DuckdbJob;

import java.sql.Connection;
import java.sql.Statement;

public final class LocalClusteringCoefficientJob extends DuckdbJob {

	/**
	 * Creates a new LocalClusteringCoefficientJob object with all mandatory parameters specified.
	 *  @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public LocalClusteringCoefficientJob(RunSpecification runSpecification, DuckdbConfiguration platformConfig,
										 String inputPath, String outputPath, Graph benchmarkGraph, Connection connection) {
		super(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
	}

	@Override
	public int execute() throws Exception {
		Statement stmt = connection.createStatement();
		if (benchmarkGraph.isDirected()) {
			stmt.execute("CREATE VIEW neighbors AS (\n" +
					"SELECT e.source AS vertex, e.target AS neighbor\n" +
					"FROM e\n" +
					"UNION\n" +
					"SELECT e.target AS vertex, e.source AS neighbor\n" +
					"FROM e)");
			stmt.execute("CREATE TABLE lcc AS (\n" +
					"SELECT\n" +
					"  id,\n" +
					"  CASE WHEN tri = 0 THEN 0.0 ELSE (CAST(tri AS float) / (deg*(deg-1))) END AS value\n" +
					"FROM (\n" +
					"    SELECT\n" +
					"    v.id AS id,\n" +
					"    (SELECT count(*) FROM neighbors WHERE neighbors.vertex = v.id) AS deg,\n" +
					"    (SELECT count(*)\n" +
					"    FROM neighbors n1\n" +
					"    JOIN neighbors n2\n" +
					"      ON n1.vertex = n2.vertex\n" +
					"    JOIN e e3\n" +
					"      ON e3.source = n1.neighbor\n" +
					"     AND e3.target = n2.neighbor\n" +
					"    WHERE n1.vertex = v.id\n" +
					"    ) AS tri\n" +
					"    FROM v\n" +
					"    ORDER BY v.id ASC\n" +
					") s\n" +
					")");
		} else {
			stmt.execute("CREATE TABLE lcc AS (\n" +
					"SELECT\n" +
					"  id,\n" +
					"  CASE WHEN deg < 2 THEN 0.0 ELSE (CAST(tri AS float) / (deg*(deg-1))) END AS value\n" +
					"FROM (\n" +
					"    SELECT\n" +
					"    v.id AS id,\n" +
					"    (SELECT count(*) FROM e WHERE e.source = v.id) AS deg,\n" +
					"    (SELECT count(*)\n" +
					"    FROM e e1\n" +
					"    JOIN e e2\n" +
					"      ON e2.source = e1.target\n" +
					"    JOIN e e3\n" +
					"      ON e3.source = e2.target\n" +
					"     AND e3.target = e1.source\n" +
					"    WHERE e1.source = v.id) AS tri\n" +
					"    FROM v\n" +
					"    ORDER BY v.id ASC\n" +
					") s\n" +
					")");
		}
		stmt.execute("COPY (SELECT id, value FROM lcc ORDER BY id) TO '/tmp/lcc.csv' WITH (DELIMITER ' ')");
		return 0;
	}

}
