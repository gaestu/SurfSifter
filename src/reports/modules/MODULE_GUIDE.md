# Report Module Development Guide

Every report module must follow this structure to ensure a consistent look and feel across all report sections. Use `_example/` as your working template.

---

## Quick Start

```
src/reports/modules/
  my_module/
    __init__.py          # Export: from .module import MyModule
    module.py            # Extends BaseReportModule
    template.html        # Jinja2 template with scoped CSS
```

1. Copy `_example/` to a new folder (no underscore prefix)
2. Rename the class and update `metadata`
3. Define your filter fields following the standard field order below
4. Implement `render()` and `get_dynamic_options()` as needed
5. Register the export in `src/reports/modules/__init__.py`

---

## Module Structure

### module.py

```python
from ..base import BaseReportModule, FilterField, FilterType, ModuleMetadata

class MyModule(BaseReportModule):

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="my_module",        # Unique, snake_case
            name="My Module",             # Human-readable, used as default title
            description="...",            # Used as default description in report
            icon="📋",                    # Single emoji
            category="Browser",           # One of: Browser, System, Timeline, Images,
        )                                 #         Downloads, URLs, Files, Documentation

    def get_filter_fields(self) -> list[FilterField]:
        return [
            # ... standard fields first, then custom fields (see below)
        ]

    def render(self, db_conn, evidence_id, config) -> str:
        # ... query data, render template
```

### template.html

```html
<div class="module-{module_id}">
    {# --- Title --- #}
    {% if show_title %}
    <h3 class="module-title">{{ title_text }}</h3>
    {% endif %}

    {# --- Description --- #}
    {% if show_description %}
    <p class="module-description">{{ description_text }}</p>
    {% endif %}

    {# --- Main Content --- #}
    ...

    {# --- Truncation Notice --- #}
    {% if is_truncated %}
    <p class="truncation-info">...</p>
    {% endif %}

    {# --- Filter Info Footer --- #}
    {% if show_filter_info %}
    <p class="filter-info">...</p>
    {% endif %}
</div>

<style>
.module-{module_id} { margin: 1em 0; }
.module-{module_id} .module-title { ... }
.module-{module_id} .module-description { ... }
/* ... all styles scoped to .module-{module_id} */
</style>
```

---

## Standard Filter Fields

Every module should include the applicable standard fields **in this order**, before any custom fields. The standard keys, types, and defaults are fixed — do not rename them.

### 1. Title Group

| Key              | Type       | Default | Purpose                                         |
|------------------|------------|---------|------------------------------------------------ |
| `show_title`     | `CHECKBOX` | `True`  | Toggle the section title on/off                 |
| `custom_title`   | `TEXT`     | `""`    | Override `metadata.name` (empty = use default)  |

```python
FilterField(
    key="show_title",
    label="Show Title",
    filter_type=FilterType.CHECKBOX,
    default=True,
    help_text="Display a title at the top of this section",
),
FilterField(
    key="custom_title",
    label="Custom Title",
    filter_type=FilterType.TEXT,
    default="",
    help_text="Custom title (leave empty for default)",
),
```

**In `render()`:**
```python
show_title = config.get("show_title", True)
custom_title = config.get("custom_title", "")
title_text = custom_title or translations.get(f"{module_id}_title", self.metadata.name)
```

### 2. Description Group

| Key                  | Type       | Default | Purpose                                                  |
|----------------------|------------|---------|--------------------------------------------------------- |
| `show_description`   | `CHECKBOX` | `True`  | Toggle the description on/off                            |
| `custom_description` | `TEXT`     | `""`    | Override `metadata.description` (empty = use default)    |

```python
FilterField(
    key="show_description",
    label="Show Description",
    filter_type=FilterType.CHECKBOX,
    default=True,
    help_text="Display a short description below the title",
),
FilterField(
    key="custom_description",
    label="Custom Description",
    filter_type=FilterType.TEXT,
    default="",
    help_text="Custom description (leave empty for default)",
),
```

**In `render()`:**
```python
show_description = config.get("show_description", True)
custom_description = config.get("custom_description", "")
description_text = custom_description or translations.get(
    f"{module_id}_description", self.metadata.description
)
```

### 3. Data Filters (module-specific)

Place module-specific data filters here. Common ones include:

| Key              | Type           | When to Use                              |
|------------------|----------------|------------------------------------------|
| `tag_filter`     | `DROPDOWN`     | Module queries tagged artifacts          |
| `browser_filter` | `BROWSER_SELECT` or `DROPDOWN` | Browser-specific data         |
| `date_from` / `date_to` | `TEXT`  | Time-bounded data                        |
| `source_filter`  | `SOURCE_SELECT`| Data from multiple extraction sources    |

**Tag filter convention:**

```python
ALL = "all"
ANY_TAG = "any_tag"

FilterField(
    key="tag_filter",
    label="Tags",
    filter_type=FilterType.DROPDOWN,
    default=ALL,
    options=[(ALL, "All"), (ANY_TAG, "Any Tag")],
    help_text="Filter by tag (specific tags loaded dynamically)",
),
```

Implement `get_dynamic_options()` to populate tags from the database.

### 4. Display Options (module-specific)

Column visibility toggles, layout options, etc.

```python
FilterField(key="show_browser", label="Show Browser", filter_type=FilterType.CHECKBOX, default=True, ...),
FilterField(key="show_profile", label="Show Profile", filter_type=FilterType.CHECKBOX, default=False, ...),
```

### 5. Sort & Limit

| Key        | Type       | Default     | Purpose                            |
|------------|------------|-------------|------------------------------------|
| `sort_by`  | `DROPDOWN` | varies      | Row ordering                       |
| `limit`    | `DROPDOWN` | `"100"`     | Max items (`"unlimited"` option)   |

```python
FilterField(
    key="sort_by",
    label="Sort By",
    filter_type=FilterType.DROPDOWN,
    default="time_desc",
    options=[("time_desc", "Time (Newest First)"), ...],
    help_text="Sort order",
),
FilterField(
    key="limit",
    label="Limit",
    filter_type=FilterType.DROPDOWN,
    default="100",
    options=[("10", "10"), ("25", "25"), ("50", "50"), ("100", "100"),
             ("250", "250"), ("500", "500"), ("unlimited", "Unlimited")],
    help_text="Maximum number of items to show",
),
```

### 6. Footer

| Key               | Type       | Default | Purpose                                |
|-------------------|------------|---------|----------------------------------------|
| `show_filter_info`| `CHECKBOX` | `False` | Show active filter summary at bottom   |

```python
FilterField(
    key="show_filter_info",
    label="Show Filter Info",
    filter_type=FilterType.CHECKBOX,
    default=False,
    help_text="Display filter criteria below the content",
),
```

---

## Complete Field Order Summary

```
┌─────────────────────────────────────┐
│  1. show_title          (standard)  │
│  2. custom_title        (standard)  │
│  3. show_description    (standard)  │
│  4. custom_description  (standard)  │
├─────────────────────────────────────┤
│  5. tag_filter          (if needed) │
│  6. browser_filter      (if needed) │
│  7. date_from / date_to (if needed) │
│  8. ... other data filters          │
├─────────────────────────────────────┤
│  9. show_browser        (if needed) │
│ 10. show_profile        (if needed) │
│ 11. ... other display toggles       │
├─────────────────────────────────────┤
│ 12. sort_by             (if needed) │
│ 13. limit               (if needed) │
├─────────────────────────────────────┤
│ 14. show_filter_info    (standard)  │
└─────────────────────────────────────┘
```

---

## Rendering Conventions

### Template Loading

Always use `Environment` + `FileSystemLoader`:

```python
from jinja2 import Environment, FileSystemLoader
from ...paths import get_module_template_dir

template_dir = get_module_template_dir(__file__)
env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
template = env.get_template("template.html")
```

### Internal Config Keys

The report generator injects these underscore-prefixed keys into `config`:

| Key                | Type   | Purpose                          |
|--------------------|--------|----------------------------------|
| `_locale`          | `str`  | Language code (e.g. `"en"`)      |
| `_translations`    | `dict` | Translation strings (`t`)        |
| `_date_format`     | `str`  | `"eu"` or `"us"`                 |
| `_case_folder`     | `str`  | Workspace path (image modules)   |
| `_evidence_label`  | `str`  | Evidence identifier              |

Access them early in `render()`:

```python
locale = config.get("_locale", "en")
translations = config.get("_translations", {})
date_format = config.get("_date_format", "eu")
```

### Title/Description Resolution in render()

```python
t = translations
show_title = config.get("show_title", True)
custom_title = config.get("custom_title", "")
title_text = custom_title or t.get("my_module_title", self.metadata.name)

show_description = config.get("show_description", True)
custom_description = config.get("custom_description", "")
description_text = custom_description or t.get("my_module_description", self.metadata.description)
```

Pass `show_title`, `title_text`, `show_description`, `description_text` to the template.

---

## Localization (locales.py)

All user-visible text in templates **must** go through the translation system so reports render correctly in both English and German.

**Source file:** `src/reports/locales.py`
**Supported locales:** `en` (English), `de` (German)

### How It Works

1. The report generator calls `get_translations(locale)` and injects the result as `config["_translations"]`
2. In `render()`, alias it as `t` and pass it to the template
3. In templates, use `t.key_name | default('English fallback')` for every user-visible string

### Adding Translation Keys for a New Module

When creating a new module, add its translation keys to **both** the `"en"` and `"de"` dicts in `locales.py`.

**Naming convention:** prefix keys with your `module_id` to avoid collisions.

```python
# In locales.py → TRANSLATIONS["en"]:

# ===================
# My Module
# ===================
"my_module_title": "My Module",
"my_module_description": "Description of what this module shows.",
"my_module_no_data": "No data found matching the filter criteria.",
"my_module_some_column": "Column Header",

# In locales.py → TRANSLATIONS["de"]:

# ===================
# My Module
# ===================
"my_module_title": "Mein Modul",
"my_module_description": "Beschreibung, was dieses Modul anzeigt.",
"my_module_no_data": "Keine Daten gefunden, die den Filterkriterien entsprechen.",
"my_module_some_column": "Spaltenüberschrift",
```

### Reuse Common Keys

Don't duplicate keys that already exist in the common/shared section of `locales.py`. These are available to all modules:

| Key | EN | DE | Usage |
|-----|----|----|-------|
| `filter` | "Filter" | "Filter" | Filter info label |
| `entries` | "entries" | "Einträge" | Generic item count |
| `showing_x_of_y` | "showing {shown} of {total}" | "zeige {shown} von {total}" | Truncation notice |
| `no_data` | "No data found." | "Keine Daten gefunden." | Generic empty state |
| `filter_all_tags` | "All tags" | "Alle Tags" | Tag filter: all |
| `filter_any_tag` | "with any tag" | "mit beliebigem Tag" | Tag filter: any |
| `filter_tagged` | `tagged "{tag}"` | `getaggt "{tag}"` | Tag filter: specific |
| `filter_sorted_by` | `sorted by {sort}` | `sortiert nach {sort}` | Sort description |
| `sort_newest_first` | "newest first" | "neueste zuerst" | Sort option |
| `sort_oldest_first` | "oldest first" | "älteste zuerst" | Sort option |
| `sort_name_az` | "name A-Z" | "Name A-Z" | Sort option |
| `sort_name_za` | "name Z-A" | "Name Z-A" | Sort option |
| `browser` | "Browser" | "Browser" | Column header |
| `profile` | "Profile" | "Profil" | Column header |
| `date` | "Date" | "Datum" | Column header |
| `name` | "Name" | "Name" | Column header |
| `value` | "Value" | "Wert" | Column header |
| `size` | "Size" | "Größe" | Column header |

See the full common section in `locales.py` (search for `# Common / shared`) for all available keys.

### Template Usage

Always use translation keys with a `| default()` fallback so templates don't break if a key is missing:

```html
{# Module-specific key with fallback #}
<h3 class="module-title">{{ t.my_module_title | default('My Module') }}</h3>

{# Reusing common key #}
<p class="empty-message">{{ t.my_module_no_data | default('No data found.') }}</p>

{# Common key with placeholder replacement #}
<p class="truncation-info">
    {{ t.showing_x_of_y | default('showing {shown} of {total}')
       | replace('{shown}', shown_count|string)
       | replace('{total}', total_count|string) }}
</p>

{# Column headers — reuse common keys where possible #}
<th>{{ t.browser | default('Browser') }}</th>
<th>{{ t.my_module_some_column | default('Column Header') }}</th>
```

### Date Formatting

Use the report date helpers, not raw datetime formatting:

```python
from ...dates import format_datetime, format_date

formatted = format_datetime(row["ts_utc"], date_format)  # date_format = "eu" or "us"
```

### Template HTML Pattern

```html
<div class="module-{module_id}">
    {% if show_title %}
    <h3 class="module-title">{{ title_text }}</h3>
    {% endif %}

    {% if show_description %}
    <p class="module-description">{{ description_text }}</p>
    {% endif %}

    <!-- main content: table, grid, or summary blocks -->

    {% if is_truncated %}
    <p class="truncation-info">
        {{ t.showing_x_of_y | default('showing {shown} of {total}')
           | replace('{shown}', shown_count|string)
           | replace('{total}', total_count|string) }}
    </p>
    {% endif %}

    {% if show_filter_info %}
    <p class="filter-info">
        <strong>{{ t.filter | default('Filter') }}:</strong> {{ filter_description }}
        <span class="entry-count">({{ total_count }} {{ t.entries | default('entries') }})</span>
    </p>
    {% endif %}
</div>
```

### CSS Scoping

All styles **must** be scoped to `.module-{module_id}`:

```css
.module-my-module { margin: 1em 0; }
.module-my-module .module-title {
    margin: 0 0 0.5em 0; font-size: 1.1em;
    font-weight: 600; color: #333;
}
.module-my-module .module-description {
    margin: 0 0 0.75em 0; color: #555;
    font-size: 0.9em; line-height: 1.4;
}
.module-my-module .filter-info {
    margin-top: 0.75em; color: #555; font-size: 0.9em;
}
.module-my-module .truncation-info {
    margin-top: 0.75em; color: #666;
    font-size: 0.85em; font-style: italic;
}
```

### Empty State

Always handle the "no data" case gracefully:

```html
{% if not items %}
<p class="empty-message">{{ t.no_data | default('No data found matching the filter criteria.') }}</p>
{% endif %}
```

---

## Checklist

Before submitting a new module:

- [ ] `module_id` is unique and snake_case
- [ ] Standard fields use the exact keys from this guide
- [ ] Field order follows the standard layout
- [ ] Template wraps everything in `<div class="module-{module_id}">`
- [ ] Title uses `show_title` + `title_text` pattern
- [ ] Description uses `show_description` + `description_text` pattern
- [ ] CSS is scoped to `.module-{module_id}`
- [ ] Empty state is handled
- [ ] `autoescape=True` is set on `Environment`
- [ ] Module is exported in `__init__.py`
- [ ] Template includes `@media print` styles if applicable
- [ ] Translation keys added to both `en` and `de` in `src/reports/locales.py`
- [ ] Template strings use `t.key | default('fallback')` pattern
- [ ] Common translation keys reused (not duplicated)
