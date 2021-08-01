package com.example.springboot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.github.database.rider.core.api.configuration.DBUnit;
import com.github.database.rider.core.api.configuration.Orthography;
import com.github.database.rider.core.api.dataset.DataSet;
import com.github.database.rider.core.api.dataset.ExpectedDataSet;
import com.github.database.rider.junit5.api.DBRider;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


/**
 * This was a test to setup how connections are leaking but it seems like these cases are never being run
 * because there's a bug in the code which resolves the class annotations. This is showcased in the 
 * NotFindingMethods test case.
 */
@SpringBootTest
@Testcontainers
@DBRider()
@DBUnit(caseInsensitiveStrategy = Orthography.LOWERCASE, cacheConnection = false, leakHunter = true)
public class LeakingConnectionsTest {

	static DataSource dataSource2;
	
	@Autowired
	private DbDAO dao;
	
	@Autowired
	private DataSource dataSource;

	@SuppressWarnings({ "rawtypes" })
	@Container
	private static GenericContainer postgresqlContainer = new PostgreSQLContainer(DockerImageName.parse("postgres:9.6"))
			.withDatabaseName("springjdbc").withUsername("foo").withPassword("secret")
			.withClasspathResourceMapping("schema.sql", "/docker-entrypoint-initdb.d/1-schema.sql", BindMode.READ_ONLY);

	@DynamicPropertySource
	static void registerPgProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", () -> String.format("jdbc:postgresql://localhost:%d/springjdbc",
				postgresqlContainer.getFirstMappedPort()));
	}

	@Configuration
	@ComponentScan(basePackages = "com.example.springboot")
	public static class WrappingDataSourceConfig {

		@Bean
		public DataSource hikari(@Value("${spring.datasource.url}") String url) {
			HikariConfig config = new HikariConfig();
			config.setDriverClassName("org.postgresql.Driver");
			config.setJdbcUrl(url);
			config.setUsername("foo");
			config.setPassword("secret");
			return new WrappingDataSource(new HikariDataSource(config));
		}
	}
	
	@BeforeEach
	@DataSet("start.yml")
	void beforeEach() {
		
	}
	
	@AfterEach
	// This should fail since we have 1,3 in the DB and the expected.yml is expecting 1,2
	@ExpectedDataSet("expected.yml")
	void afterEach() {
		dataSource2 = dataSource;
	}
	
	@AfterAll
	static void afterAll() {
		WrappingDataSource wrappingDataSource = (WrappingDataSource) dataSource2;
		assertEquals(0, wrappingDataSource.liveConnections.get());
	}

	@Test
	@Order(1)
	void test1() {
		assertEquals(1, dao.update(2));
		assertTrue(dataSource instanceof WrappingDataSource);
		WrappingDataSource wrappingDataSource = (WrappingDataSource) dataSource;
		assertEquals(0, wrappingDataSource.liveConnections.get());
	}


	@Test
	@Order(2)
	void test2() {
		assertEquals(1, dao.update(2));
		assertTrue(dataSource instanceof WrappingDataSource);
		WrappingDataSource wrappingDataSource = (WrappingDataSource) dataSource;
		assertEquals(0, wrappingDataSource.liveConnections.get());
	}
	

	@Test
	@Order(3)
	void test3() {
		assertEquals(1, dao.update(2));
		assertTrue(dataSource instanceof WrappingDataSource);
		WrappingDataSource wrappingDataSource = (WrappingDataSource) dataSource;
		assertEquals(0, wrappingDataSource.liveConnections.get());
	}
	
	private static class WrappingDataSource implements DataSource {

		 private final DataSource delegate;
		 private final AtomicInteger liveConnections = new AtomicInteger();

		WrappingDataSource(DataSource delegate){
			 this.delegate = delegate;
		 }
		
		
		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return delegate.getParentLogger();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return delegate.isWrapperFor(iface);
		}

		@Override
		public Connection getConnection() throws SQLException {
			WrappingConnection wrappingConnection = new WrappingConnection(delegate.getConnection(), this);
			new Exception("getConnection "+wrappingConnection+ " nr:"+liveConnections.incrementAndGet()).printStackTrace(System.out);
			return wrappingConnection;
		}

		@Override
		public Connection getConnection(String username, String password) throws SQLException {
			WrappingConnection wrappingConnection = new WrappingConnection(delegate.getConnection(username, password), this);
			new Exception("getConnection(username, password) "+ wrappingConnection+ " nr:" + liveConnections.incrementAndGet()).printStackTrace(System.out);
			return wrappingConnection;
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return delegate.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			delegate.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			delegate.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return delegate.getLoginTimeout();
		}


		public int closeConnection() {
			return liveConnections.decrementAndGet();
		}
	}
	
	private static class WrappingConnection implements Connection {

		private final Connection delegate;
		private final WrappingDataSource datasource;
		
		public WrappingConnection(Connection delegate, WrappingDataSource datasource) {
			this.delegate = delegate;
			this.datasource = datasource;
		}
		
		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return delegate.isWrapperFor(iface);
		}

		@Override
		public Statement createStatement() throws SQLException {
			return delegate.createStatement();
		}

		@Override
		public PreparedStatement prepareStatement(String sql) throws SQLException {
			return delegate.prepareStatement(sql);
		}

		@Override
		public CallableStatement prepareCall(String sql) throws SQLException {
			return delegate.prepareCall(sql);
		}

		@Override
		public String nativeSQL(String sql) throws SQLException {
			return delegate.nativeSQL(sql);
		}

		@Override
		public void setAutoCommit(boolean autoCommit) throws SQLException {
			delegate.setAutoCommit(autoCommit);
		}

		@Override
		public boolean getAutoCommit() throws SQLException {
			return delegate.getAutoCommit();
		}

		@Override
		public void commit() throws SQLException {
			delegate.commit();
		}

		@Override
		public void rollback() throws SQLException {
			delegate.rollback();
		}

		@Override
		public void close() throws SQLException {			
			new Exception(this + ".close() left checkedout:"+ datasource.closeConnection()).printStackTrace(System.out);
			delegate.close();
		}

		@Override
		public boolean isClosed() throws SQLException {
			return delegate.isClosed();
		}

		@Override
		public DatabaseMetaData getMetaData() throws SQLException {
			return delegate.getMetaData();
		}

		@Override
		public void setReadOnly(boolean readOnly) throws SQLException {
			delegate.setReadOnly(readOnly);
		}

		@Override
		public boolean isReadOnly() throws SQLException {
			return delegate.isReadOnly();
		}

		@Override
		public void setCatalog(String catalog) throws SQLException {
			delegate.setCatalog(catalog);
		}

		@Override
		public String getCatalog() throws SQLException {
			return delegate.getCatalog();
		}

		@Override
		public void setTransactionIsolation(int level) throws SQLException {
			delegate.setTransactionIsolation(level);
		}

		@Override
		public int getTransactionIsolation() throws SQLException {
			return delegate.getTransactionIsolation();
		}

		@Override
		public SQLWarning getWarnings() throws SQLException {
			return delegate.getWarnings();
		}

		@Override
		public void clearWarnings() throws SQLException {
			delegate.clearWarnings();
		}

		@Override
		public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
			return delegate.createStatement(resultSetType, resultSetConcurrency);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
				throws SQLException {
			return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
				throws SQLException {
			return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
		}

		@Override
		public Map<String, Class<?>> getTypeMap() throws SQLException {
			return delegate.getTypeMap();
		}

		@Override
		public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
			delegate.setTypeMap(map);
		}

		@Override
		public void setHoldability(int holdability) throws SQLException {
			delegate.setHoldability(holdability);
		}

		@Override
		public int getHoldability() throws SQLException {
			return delegate.getHoldability();
		}

		@Override
		public Savepoint setSavepoint() throws SQLException {
			return delegate.setSavepoint();
		}

		@Override
		public Savepoint setSavepoint(String name) throws SQLException {
			return delegate.setSavepoint(name);
		}

		@Override
		public void rollback(Savepoint savepoint) throws SQLException {
			delegate.rollback(savepoint);
		}

		@Override
		public void releaseSavepoint(Savepoint savepoint) throws SQLException {
			delegate.releaseSavepoint(savepoint);
		}

		@Override
		public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
				throws SQLException {
			return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) throws SQLException {
			return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) throws SQLException {
			return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
			return delegate.prepareStatement(sql, autoGeneratedKeys);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
			return delegate.prepareStatement(sql, columnIndexes);
		}

		@Override
		public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
			return delegate.prepareStatement(sql, columnNames);
		}

		@Override
		public Clob createClob() throws SQLException {
			return delegate.createClob();
		}

		@Override
		public Blob createBlob() throws SQLException {
			return delegate.createBlob();
		}

		@Override
		public NClob createNClob() throws SQLException {
			return delegate.createNClob();
		}

		@Override
		public SQLXML createSQLXML() throws SQLException {
			return delegate.createSQLXML();
		}

		@Override
		public boolean isValid(int timeout) throws SQLException {
			return delegate.isValid(timeout);
		}

		@Override
		public void setClientInfo(String name, String value) throws SQLClientInfoException {
			delegate.setClientInfo(name, value);
		}

		@Override
		public void setClientInfo(Properties properties) throws SQLClientInfoException {
			delegate.setClientInfo(properties);
		}

		@Override
		public String getClientInfo(String name) throws SQLException {
			return delegate.getClientInfo(name);
		}

		@Override
		public Properties getClientInfo() throws SQLException {
			return delegate.getClientInfo();
		}

		@Override
		public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
			return delegate.createArrayOf(typeName, elements);
		}

		@Override
		public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
			return delegate.createStruct(typeName, attributes);
		}

		@Override
		public void setSchema(String schema) throws SQLException {
			delegate.setSchema(schema);
		}

		@Override
		public String getSchema() throws SQLException {
			return delegate.getSchema();
		}

		@Override
		public void abort(Executor executor) throws SQLException {
			delegate.abort(executor);
		}

		@Override
		public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
			delegate.setNetworkTimeout(executor, milliseconds);
		}

		@Override
		public int getNetworkTimeout() throws SQLException {
			return delegate.getNetworkTimeout();
		}
		
	}
}
