package com.spark.ml.demo;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class AdServerProductRequest implements Serializable {

	private static final long serialVersionUID = 1L;

	private String id;
	private String click;
	private String hour;
	private String c1;
	private String banner_pos;
	private String site_id;
	private String site_domain;
	private String site_category;
	private String app_id;
	private String app_domain;
	private String app_category;
	private String device_id;
	private String device_ip;
	private String device_model;
	private String device_type;
	private String device_conn_type;
	private String c14;
	private String c15;
	private String c16;
	private String c17;
	private String c18;
	private String c19;
	private String c20;
	private String c21;

	public AdServerProductRequest() {

	}

	public AdServerProductRequest(String id, String click, String hour, String c1, String banner_pos, String site_id, String site_domain,
			String site_category, String app_id, String app_domain, String app_category, String device_id, String device_ip, String device_model,
			String device_type, String device_conn_type, String c14, String c15, String c16, String c17, String c18, String c19, String c20,
			String c21) {
		super();
		this.id = id;
		this.click = click;
		this.hour = hour;
		this.c1 = c1;
		this.banner_pos = banner_pos;
		this.site_id = site_id;
		this.site_domain = site_domain;
		this.site_category = site_category;
		this.app_id = app_id;
		this.app_domain = app_domain;
		this.app_category = app_category;
		this.device_id = device_id;
		this.device_ip = device_ip;
		this.device_model = device_model;
		this.device_type = device_type;
		this.device_conn_type = device_conn_type;
		this.c14 = c14;
		this.c15 = c15;
		this.c16 = c16;
		this.c17 = c17;
		this.c18 = c18;
		this.c19 = c19;
		this.c20 = c20;
		this.c21 = c21;
	}

}
