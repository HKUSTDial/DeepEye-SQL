WITH file_contents AS (
    SELECT 
        f."path",
        c."content"
    FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_FILES" f
    INNER JOIN "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS" c
        ON f."id" = c."id"
    WHERE c."content" IS NOT NULL AND c."content" != ''
),
files_with_language AS (
    SELECT 
        CASE
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('asm', 'nasm') THEN 'Assembly'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('c', 'h') THEN 'C'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'cs' THEN 'C#'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('c++', 'cpp', 'h++', 'hpp') THEN 'C++'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'css' THEN 'CSS'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'clj' THEN 'Clojure'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'lisp' THEN 'Common Lisp'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'd' THEN 'D'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'dart' THEN 'Dart'
            WHEN LOWER("path") LIKE '%dockerfile%' THEN 'Dockerfile'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('ex', 'exs') THEN 'Elixir'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'erl' THEN 'Erlang'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'go' THEN 'Go'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'groovy' THEN 'Groovy'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('html', 'htm') THEN 'HTML'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'hs' THEN 'Haskell'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'hx' THEN 'Haxe'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'json' THEN 'JSON'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'java' THEN 'Java'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('js', 'cjs') THEN 'JavaScript'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'jl' THEN 'Julia'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('kt', 'ktm', 'kts') THEN 'Kotlin'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'lua' THEN 'Lua'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('matlab', 'm') THEN 'MATLAB'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('md', 'markdown', 'mdown') THEN 'Markdown'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'php' THEN 'PHP'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('ps1', 'psd1', 'psm1') THEN 'PowerShell'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'py' THEN 'Python'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'r' THEN 'R'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'rb' THEN 'Ruby'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'rs' THEN 'Rust'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'scss' THEN 'SCSS'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'sql' THEN 'SQL'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'sass' THEN 'Sass'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'scala' THEN 'Scala'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('sh', 'bash') THEN 'Shell'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'swift' THEN 'Swift'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'ts' THEN 'TypeScript'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'vue' THEN 'Vue'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) = 'xml' THEN 'XML'
            WHEN LOWER(SPLIT_PART("path", '.', -1)) IN ('yml', 'yaml') THEN 'YAML'
            ELSE NULL
        END AS language
    FROM file_contents
)
SELECT 
    language,
    COUNT(*) AS file_count
FROM files_with_language
WHERE language IS NOT NULL
GROUP BY language
ORDER BY file_count DESC
LIMIT 10