(function () {
  if (window.__hookaidoCommandPaletteBound) {
    return;
  }
  window.__hookaidoCommandPaletteBound = true;

  function isEditableTarget(target) {
    if (!target) {
      return false;
    }
    if (target.isContentEditable) {
      return true;
    }
    var tag = target.tagName ? target.tagName.toLowerCase() : "";
    return tag === "input" || tag === "textarea" || tag === "select";
  }

  function getSearchInput() {
    return document.querySelector('input[data-md-component="search-query"]');
  }

  function openSearchDialog() {
    var toggle = document.querySelector('input[data-md-toggle="search"]');
    if (toggle) {
      if (!toggle.checked) {
        toggle.checked = true;
        toggle.dispatchEvent(new Event("change", { bubbles: true }));
      }
      return;
    }
    var fallbackOpenButton = document.querySelector('label[for="__search"]');
    if (fallbackOpenButton) {
      fallbackOpenButton.click();
    }
  }

  function focusSearchInput() {
    openSearchDialog();
    window.setTimeout(function () {
      var searchInput = getSearchInput();
      if (!searchInput) {
        return;
      }
      searchInput.focus();
      searchInput.select();
    }, 30);
  }

  function applySearchPlaceholderHint() {
    var searchInput = getSearchInput();
    if (!searchInput) {
      return;
    }
    searchInput.placeholder = "Search docs (Ctrl+K)";
  }

  function onKeyDown(event) {
    if (!(event.ctrlKey || event.metaKey)) {
      return;
    }
    if (event.key.toLowerCase() !== "k") {
      return;
    }
    if (isEditableTarget(event.target)) {
      return;
    }
    event.preventDefault();
    focusSearchInput();
  }

  document.addEventListener("keydown", onKeyDown);
  document.addEventListener("DOMContentLoaded", applySearchPlaceholderHint);

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(applySearchPlaceholderHint);
  }
})();
