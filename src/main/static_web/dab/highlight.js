$(document).ready(function() {
  console.log("hello world");
  //var host = "http://disambiguationhost";
  $("#siteSub").append('<img id="loadinghighlights" src="' + dabHost + 'waiting.gif"><div id="foundhighlights"></div>');
  $('head').append('<link rel="stylesheet" href="' + dabHost + 'dabstyle.css" type="text/css" />');
  var wikipediaAPI = dabHost + 'dablinks';

  var highlighter = function(highlights_needed) {
    var found = 0;
    $("#bodyContent a").each(function(x,y) {
      if (jQuery.inArray(y.title, highlights_needed) >= 0) {
        $(y).addClass("ambiguous");
//        console.log(y.title);
        found ++;
      }
    });
    $("#loadinghighlights").hide();
    var fh = $("#foundhighlights")
    found += fh.data("linkcount") || 0;
    fh.data("linkcount", found);
    var msg;
    if (found == 0) {
      msg = "There were no ambiguous links found.";
    } else if (found == 1) {
      msg = "There was <span class='ambiguous'>one</span> ambiguous link found.";
    } else {
      msg = "There were <span class='ambiguous'>" + found + "</span> ambiguous links found.";
    }
    fh.html(msg);
  };

  var check_for_highlights = function(links) { 
    if (links.length > 0) {
      $.getJSON(wikipediaAPI + "?callback=?", { "links" : links }, highlighter);
    }
  };
  
  var links = [];

  $("#bodyContent a").each(function (index,y) {
    if (y.title && y.title != "" && y.getAttribute('class') != "new") {
      links.push({"title":y.title, "text":$(y).text()});
    }
  });
  links.sort();
  var last = null;
  var titles_to_query = [];
  for (var index in links) {
    var title = links[index];
    if (title != last) {
      titles_to_query.push(title);
      if (titles_to_query.length == 100) {
        check_for_highlights(titles_to_query);
        titles_to_query.length = 0;
      }
    }
    last = title;
  }
  check_for_highlights(titles_to_query);
  
});

