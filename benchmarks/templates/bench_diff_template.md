## Benchmark comparison
{% for (path_main, users_main), (path_curr, users_curr) in zip %}
### {{method}} {{ path_main }}

| User | Time(ms) Main | Time(ms) Current | Ratio Main/Current | Size(Kb) Main | Size(Kb) Current |
| -- | -- | -- | -- | -- | -- |
{% for i in range(users_main|length) %}| {{users_main[i].get("username")}} | {{users_main[i].get('time')}} | {{users_curr[i].get('time')}} | <span style="{% if (users_main[i].get('time') / users_curr[i].get('time')) > 2* (users_main[i].get('time') / users_curr[i].get('time')) %} color:red; {% endif %}"> {{ "%.2f"| format(users_main[i].get('time') / users_curr[i].get('time')) }} </span>| {{users_main[i].get('size')}} |{{users_curr[i].get('size')}} |
{%endfor%}

{% endfor %}
