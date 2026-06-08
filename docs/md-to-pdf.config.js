// Config for `just design-pdf` (md-to-pdf). Renders docs/DESIGN.md with a
// print stylesheet tuned so wide tables wrap inside A4 instead of overflowing.
module.exports = {
  stylesheet: ['docs/design-pdf.css'],
  body_class: 'markdown-body',
  pdf_options: {
    format: 'A4',
    landscape: true,
    margin: '12mm 14mm',
    printBackground: true,
  },
};
