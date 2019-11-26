package com.tesco.aqueduct.pipe.api

import spock.lang.Specification

import java.time.ZoneId
import java.time.ZonedDateTime

class JsonHelperSpec extends Specification {
	def "Converts an object to JSON String, empty fields are ignored"() {
		given: "An Object"
		def obj = new TestObject("test")
		when: "I ask for a JSON representation of the object"
		def result = JsonHelper.toJson(obj)
		then: "A string is returned correctly"
		result == """{"name":"test"}"""
	}

	def "Numbers are written as Strings"() {
		given: "An Object"
		def obj = new TestObject("test", 12)
		when: "I ask for a JSON representation of the object"
		def result = JsonHelper.toJson(obj)
		then: "A string is returned correctly"
		result == """{"name":"test","number":"12"}"""
	}

	def "Dates are written as..."() {
		given: "An Object with a date"
		def date = ZonedDateTime.of(2019, 12, 13, 14, 25, 7, 0, ZoneId.systemDefault())
		//def date = new Date(119, 11, 13, 14, 25, 7)
		def obj = new TestObject("test", null, date)
		when: "I ask for a JSON representation of the object"
		def result = JsonHelper.toJson(obj)
		then: "A string is returned correctly"
		result == """{"name":"test","date":"2019-12-13T14:25:07Z"}"""
	}

	def "Objects are correctly deserialised"() {
		given: "A valid json string"
		def input = """{"name":"test","date":"2019-12-13T14:25:07Z"}"""
		when: "I ask for the Object representation"
		def result = JsonHelper.fromJson(input, TestObject.class)
		then: "The result is correct"
		def date = ZonedDateTime.of(2019, 12, 13, 14, 25, 7, 0, ZoneId.systemDefault())
		def expected = new TestObject("test", null, date)
		result == expected
	}

	def "Lists of Objects are correctly serialised"() {
		given: "A valid json string for a List"
		def date = ZonedDateTime.of(2019, 12, 13, 14, 25, 7, 0, ZoneId.systemDefault())
		def obj1 = new TestObject("test-1", null, date)
		def obj2 = new TestObject("test-2", null, date)
		when: "I ask for the List<> representation"
		def result = JsonHelper.toJson([obj1, obj2])
		then: "The result is correct"
		result == """[{"name":"test-1","date":"2019-12-13T14:25:07Z"},{"name":"test-2","date":"2019-12-13T14:25:07Z"}]"""
	}

	def "Lists of Objects are correctly deserialised"() {
		given: "A valid json string for a List"
		def input = """[{"name":"test-1","date":"2019-12-13T14:25:07Z"},{"name":"test-2","date":"2019-12-13T14:25:07Z"}]"""
		when: "I ask for the List<> representation"
		def result = JsonHelper.listFromJson(input, TestObject.class)
		then: "The result is correct"
		def date = ZonedDateTime.of(2019, 12, 13, 14, 25, 7, 0, ZoneId.systemDefault())
		def obj1 = new TestObject("test-1", null, date)
		def obj2 = new TestObject("test-2", null, date)
		result == [obj1, obj2]
	}

	static class TestObject {
		final String name
		final Integer number
		final ZonedDateTime date

		TestObject() {
			this(null)
		}

		TestObject(def name) {
			this(name, null)
		}

		TestObject(def name, def number) {
			this(name, number, null)
		}

		TestObject(def name, def number, def date) {
			this.name = name
			this.number = number
			this.date = date
		}

		@Override
		boolean equals(Object otherObj) {
			if (!otherObj instanceof TestObject) return false
			TestObject other = (TestObject) otherObj
			return other.name == name &&
				other.number == number &&
				(Objects.equals(date, other.date) || (date != null && date.isEqual(other.date)))
		}
	}
}

