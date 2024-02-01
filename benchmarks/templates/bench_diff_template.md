## Benchmark comparison
{% for (path_main, users_main), (path_curr, users_curr) in zip %}
### {{method}} {{ path_main }}

| User | Time `{{currentbranch}}` (ms) | Time `main` (ms) | Ratio `{{currentbranch}}`/`main` | Size (Kb) `{{currentbranch}}` | Size `main` (Kb) |
| -- | -- | -- | -- | -- | -- |
{% for i in range(users_main|length) %}| {{users_main[i].get("username")}} | {{users_curr[i].get('time')}} | {{users_main[i].get('time')}} | <span style="{% if (users_main[i].get('time') / users_curr[i].get('time')) > 2 * users_main[i].get('time') %} color:red; {% endif %}"> {{ "%.2f"| format(users_curr[i].get('time') / users_main[i].get('time')) }} </span>| {{users_curr[i].get('size')}} |{{users_main[i].get('size')}} |
{%endfor%}

{% endfor %}

| Path | Status | Exception |
| -- | -- | -- |
{% for exception in exceptions %}| {{exception["path"]}} | {{exception["status"]}} | {{exception["exception"] ~ " " ~ exception["detail"] ~ string}} |
{% endfor %}
