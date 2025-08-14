{# This is a Jinja code generated for select statement#}
{% set max_number = 10 %}
{% for i in range(max_number) %}
    SELECT {{ i }} AS number
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}