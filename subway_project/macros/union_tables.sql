{% macro union_sat_datas(mass_tables, pseudo_col)  %}
	{% for tbls in mass_tables %}
		select {% for col in range(1,tbls|length) %} {{tbls[col]}} {% if col-2 >= 0 %} {{pseudo_col[col-2]}} {% endif %} {% if not loop.last %},{% endif %} {% endfor %} 
		from {{tbls[0]}} 
		{% if not loop.last %} union all {% endif %}
	{% endfor %}
{% endmacro %}