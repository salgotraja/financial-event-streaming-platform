Diagram sources and exports.

Each diagram is a draw.io file plus an SVG exported from it. The SVG is what the guide embeds; the
.drawio file is the editable source and is what you change.

Regenerate one after editing:

    drawio -x -f svg -e -b 10 --embed-svg-fonts false -o NAME.svg NAME.drawio

--embed-svg-fonts false is not optional. With font embedding on, draw.io writes a raster PNG fallback
for every text label and the file grows from about 40KB to over 1MB.

-e embeds the diagram XML in the SVG, so an exported file can be reopened in draw.io if the source is
ever lost.

Regenerate all of them:

    for f in *.drawio; do
      drawio -x -f svg -e -b 10 --embed-svg-fonts false -o "${f%.drawio}.svg" "$f"
    done

Note on labels: draw.io renders vertex values as HTML, so angle brackets in a label are parsed as
tags and disappear. Write "List of Grant" rather than "List<Grant>".
