var table = "<table><tr><th>popular_quality_artists</th><th>popular_quantity_artists</th><th>quality_artists</th><th>quantity_artists</th></tr>";
table += "<tr>";
for (var j=0; j<vars.length; j++) {
    table += "<td>";
    for (var i = 0; i < vars[j].length; i++) {
        {
            table += "<div><a href='./ugoira/" + vars[j][i][0] + "/gifs/'>" + vars[j][i][0]+" ("+vars[j][i][1]+")"+ "</a></div>";
        }
    }
    table += "</td>";
}
table += "</tr></table>";
$("body").append(table);
html = null;