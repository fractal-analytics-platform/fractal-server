## Benchmark comparison

Current branch: `{{currentbranch}}`

{% for (path_main, users_main), (path_curr, users_curr) in zip %}
### {{method}} {{ path_main }}

| User | Time _current_ (ms) | Time `main` (ms) | Ratio _current_/`main` | Size _current_ (Kb) | Size `main` (Kb) |
| -- | -- | -- | -- | -- | -- |
{% for i in range(users_main|length) %}| {{users_main[i].get("username")}} | {{users_curr[i].get('time')}} | {{users_main[i].get('time')}} | <span style="{% if (users_curr[i].get('time') / users_main[i].get('time')) > 2 %} color:red; {% endif %}"> {{ "%.2f"| format(users_curr[i].get('time') / users_main[i].get('time')) }} </span>| {{users_curr[i].get('size')}} |{{users_main[i].get('size')}} |
{%endfor%}

{% endfor %}

| Path | Status | Exception |
| -- | -- | -- |
{% for exception in exceptions %}| {{exception["path"]}} | {{exception["status"]}} | {{exception["exception"] ~ " " ~ exception["detail"] ~ string}} |
{% endfor %}
