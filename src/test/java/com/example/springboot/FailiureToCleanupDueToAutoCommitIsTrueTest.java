package com.example.springboot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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

@SpringBootTest
@Testcontainers
@DBRider()
@DBUnit(caseInsensitiveStrategy = Orthography.LOWERCASE, raiseExceptionOnCleanUp = true)
public class FailiureToCleanupDueToAutoCommitIsTrueTest {

	static private DbDAO dao2;
	
	@Autowired
	private DbDAO dao;

	@SuppressWarnings({ "rawtypes" })
	@Container
	private static GenericContainer postgresqlContainer = new PostgreSQLContainer(DockerImageName.parse("postgres:9.6"))
			.withDatabaseName("springjdbc")
			.withUsername("foo")
			.withPassword("secret")
			.withClasspathResourceMapping("schema.sql", "/docker-entrypoint-initdb.d/1-schema.sql", BindMode.READ_ONLY);

	@DynamicPropertySource
	static void registerPgProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", () -> String.format("jdbc:postgresql://localhost:%d/springjdbc",
				postgresqlContainer.getFirstMappedPort()));
	}
	
	@AfterAll
	static void cleanup() {
		List<Integer> all = dao2.findAll();
		assertThat(all).hasSameElementsAs(List.of());
	}

	@Test
	@DataSet(value = "start.yml", cleanAfter = true)
	@ExpectedDataSet("expected.yml")
	void failsToClearDueToConnectionHasSetAutoCommitEqualsToTrue() {
		dao2 = dao;
		assertEquals(1, dao.update(2));
	}
	
	@Test
	@DataSet(value = "start.yml", cleanAfter = true, tableOrdering = "test")
	@ExpectedDataSet("expected.yml")
	void alsoFailsToClearDueToConnectionHasSetAutoCommitEqualsToTrue() {
		dao2 = dao;
		assertEquals(1, dao.update(2));
		assertEquals(1, dao.update2(2));
	}

}
