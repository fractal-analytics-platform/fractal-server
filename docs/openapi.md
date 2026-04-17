<!---
Original source:
mkdocs-render-swagger-plugin, Copyright (c) 2020 Bar Harel, MIT Licensed
https://github.com/bharel/mkdocs-render-swagger-plugin
--->
<link type="text/css" rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
<div id="swagger-ui-1">
</div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" charset="UTF-8"></script>
<script>
    SwaggerUIBundle({
      url: 'http://127.0.0.1:8000/openapi.json',
      dom_id: '#swagger-ui-1',
    })
</script>
