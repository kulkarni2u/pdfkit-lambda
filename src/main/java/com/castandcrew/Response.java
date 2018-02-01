package com.castandcrew;

import java.util.HashMap;
import java.util.Map;

public class Response {

	private final String message;
	private final Map<String, Object> input;

	public Response(String message, Map<String, Object> input) {
		this.message = message;
		this.input = input;
	}

	public Response(String message, String contentType) {
		this.message = message;
		this.input = new HashMap<>();
		this.input.put("contentType", contentType);
	}

	public String getMessage() {
		return this.message;
	}

	public Map<String, Object> getInput() {
		return this.input;
	}

	@Override
	public String toString() {
		return "Response{" +
				"message='" + message + '\'' +
				", input=" + input +
				'}';
	}
}
