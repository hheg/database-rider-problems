package com.example.springboot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NotFindingMethods {

	
	@AfterEach
	void teardown() {
		
	}
	
	@BeforeEach
	void setup() {
		
	}
	
	@BeforeAll
	static void setupAll() {
		
	}
	
	@AfterAll
	static void teardownAll() {
		
	}
	
	@Test
	void testGettingAfterEach() {
		Set<Method> methods = findCallbackMethods(getClass(), AfterEach.class);
		assertEquals(1, methods.size());
	}
	
	@Test
	void testGettingBeforeEach() {
		Set<Method> methods = findCallbackMethods(getClass(), BeforeEach.class);
		assertEquals(1, methods.size());
	}
	
	@Test
	void testGettingBeforeAll() {
		Set<Method> methods = findCallbackMethods(getClass(), BeforeAll.class);
		assertEquals(1, methods.size());
	}
	
	@Test
	void testGettingAfterAll() {
		Set<Method> methods = findCallbackMethods(getClass(), AfterAll.class);
		assertEquals(1, methods.size());
	}
	
	/*
	 * This is the method in DBunitExtension.findCallbackMethods which finds the callback methods
	 * but it fails to actually find the teardown() method declared above. This causes the annotations
	 * on @BeforeEach, @AfterEach, @BeforeAll and @AfterAll not to be run.
	 */
	private <T extends Annotation> Set<Method> findCallbackMethods(Class<?> testClass, Class<T> callback) {
		final Set<Method> methods = new HashSet<>();
		Stream.of(testClass.getSuperclass().getMethods(), testClass.getMethods())
				.flatMap(Stream::of)
				.filter(m -> m.getAnnotation(callback) != null)
				.forEach(m -> methods.add((Method) m));
		return methods;
	}
}
