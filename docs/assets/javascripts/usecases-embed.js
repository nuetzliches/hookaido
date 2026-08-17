document.addEventListener('DOMContentLoaded', function () {
  var frame = document.getElementById('excalidraw-frame');
  if (!frame) return;
  // Resolve usecases.html relative to the parent directory of the current page (/usecases/ -> /)
  frame.src = new URL('../usecases.html', window.location.href).href;
});
