export function renderMaterialIcons(root = document) {
  const iconElements = root.querySelectorAll('.ep-icon');
  iconElements.forEach((iconElement) => {
    const iconName = (iconElement.dataset.iconName || iconElement.textContent || '').trim();
    if (!iconName) return;
    iconElement.dataset.iconName = iconName;
    iconElement.textContent = iconName;
    iconElement.classList.add('material-icons-outlined');
    iconElement.setAttribute('aria-hidden', 'true');
  });
}

export function escapeHtml(value) {
  return (value || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

export function escapeAttr(value) {
  return escapeHtml(value).replaceAll('\n', '&#10;');
}

export function openDialog(dialogElement) {
  if (typeof dialogElement.showModal === 'function') {
    dialogElement.showModal();
  } else {
    dialogElement.setAttribute('open', 'open');
  }
}

export function closeDialog(dialogElement) {
  if (typeof dialogElement.close === 'function' && dialogElement.hasAttribute('open')) {
    dialogElement.close();
  } else {
    dialogElement.removeAttribute('open');
  }
}

export function highlightCode(codeElement) {
  if (window.Prism && typeof window.Prism.highlightElement === 'function') {
    window.Prism.highlightElement(codeElement);
  }
}
