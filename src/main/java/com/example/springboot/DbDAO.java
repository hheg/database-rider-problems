package com.example.springboot;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DbDAO {

	@Autowired
	public JdbcTemplate template;

	// NB No transaction
	public int update(int value) {
		return template.update("INSERT INTO test (id) VALUES (?)", value);
	}
	public int update2(int value) {
		return template.update("INSERT INTO test2 (id) VALUES (?)", value);
	}

	public List<Integer> findAll() {
		return template.query("select * from test2", (rs, rowNum) -> rs.getInt("id"));
	}
}
