var checkout = {};

$(document).ready(function() {
  var $messages = $('.messages-content'),
    d, h, m,
    i = 0;
  // Keep one stable Lex session per browser page load.
  var clientSessionId = 'web-' + Date.now() + '-' + Math.random().toString(36).slice(2, 10);
  var clientUserId = getOrCreateUserId();
  var RETURNING_USER_PROBE = '__returning_user_check__';

  function getOrCreateUserId() {
    var key = 'conciergeUserId';
    try {
      var existing = window.localStorage.getItem(key);
      if (existing && existing.trim() !== '') {
        return existing;
      }
      var generated = 'user-' + Date.now() + '-' + Math.random().toString(36).slice(2, 12);
      window.localStorage.setItem(key, generated);
      return generated;
    } catch (e) {
      return 'ephemeral-' + Date.now() + '-' + Math.random().toString(36).slice(2, 12);
    }
  }

  $(window).load(function() {
    $messages.mCustomScrollbar();
    insertResponseMessage('Hi there, I\'m your personal Concierge. How can I help?');
    checkReturningUserRecommendation();
  });

  function updateScrollbar() {
    $messages.mCustomScrollbar("update").mCustomScrollbar('scrollTo', 'bottom', {
      scrollInertia: 10,
      timeout: 0
    });
  }

  function setDate() {
    d = new Date()
    if (m != d.getMinutes()) {
      m = d.getMinutes();
      $('<div class="timestamp">' + d.getHours() + ':' + m + '</div>').appendTo($('.message:last'));
    }
  }

  function callChatbotApi(message) {
    // params, body, additionalParams
    return sdk.chatbotPost({}, {
      userId: clientUserId,
      sessionId: clientSessionId,
      messages: [{
        type: 'unstructured',
        unstructured: {
          id: clientSessionId,
          userId: clientUserId,
          text: message
        }
      }]
    }, {});
  }

  function extractResponseData(response) {
    var data = response.data;
    if (data && data.body && typeof data.body === 'string') {
      try {
        data = JSON.parse(data.body);
      } catch (e) {
        // keep original response shape if body isn't JSON
      }
    }
    return data;
  }

  function checkReturningUserRecommendation() {
    callChatbotApi(RETURNING_USER_PROBE)
      .then((response) => {
        var data = extractResponseData(response);
        if (!data || !data.messages || data.messages.length === 0) {
          return;
        }
        var first = data.messages[0];
        if (first.type === 'unstructured' && first.unstructured && first.unstructured.text) {
          insertResponseMessage(first.unstructured.text);
        }
      })
      .catch((error) => {
        console.log('returning user probe failed', error);
      });
  }

  function formatChatbotError(error) {
    var status = error && error.response ? error.response.status : null;
    var data = error && error.response ? error.response.data : null;
    var apiError = null;
    if (data && typeof data === 'object') {
      apiError = data.error || data.message || null;
    } else if (typeof data === 'string') {
      apiError = data;
    }

    if (!status) {
      return 'Network/CORS error. Check API Gateway CORS and invoke URL.';
    }
    if (status === 403) {
      return '403 Forbidden: wrong stage/path or missing API key.';
    }
    if (status === 502) {
      return '502 from API Gateway: Lambda integration/config issue.';
    }
    if (status >= 500) {
      return 'Server error (' + status + ')' + (apiError ? ': ' + apiError : '');
    }
    return 'Request failed (' + status + ')' + (apiError ? ': ' + apiError : '');
  }

  function insertMessage() {
    msg = $('.message-input').val();
    if ($.trim(msg) == '') {
      return false;
    }
    $('<div class="message message-personal">' + msg + '</div>').appendTo($('.mCSB_container')).addClass('new');
    setDate();
    $('.message-input').val(null);
    updateScrollbar();

    callChatbotApi(msg)
      .then((response) => {
        console.log(response);
        var data = extractResponseData(response);

        if (data.messages && data.messages.length > 0) {
          console.log('received ' + data.messages.length + ' messages');

          var messages = data.messages;

          for (var message of messages) {
            if (message.type === 'unstructured') {
              insertResponseMessage(message.unstructured.text);
            } else if (message.type === 'structured' && message.structured.type === 'product') {
              var html = '';

              insertResponseMessage(message.structured.text);

              setTimeout(function() {
                html = '<img src="' + message.structured.payload.imageUrl + '" witdth="200" height="240" class="thumbnail" /><b>' +
                  message.structured.payload.name + '<br>$' +
                  message.structured.payload.price +
                  '</b><br><a href="#" onclick="' + message.structured.payload.clickAction + '()">' +
                  message.structured.payload.buttonLabel + '</a>';
                insertResponseMessage(html);
              }, 1100);
            } else {
              console.log('not implemented');
            }
          }
        } else {
          var detail = (data && (data.error || data.message)) ? (': ' + (data.error || data.message)) : '';
          insertResponseMessage('Oops, backend returned no chatbot messages' + detail);
        }
      })
      .catch((error) => {
        var errorMsg = formatChatbotError(error);
        console.log('chatbot request failed', {
          error: error,
          invokeUrl: sdk && sdk.__invokeUrl ? sdk.__invokeUrl : 'unknown'
        });
        insertResponseMessage('Oops, request failed. ' + errorMsg);
      });
  }

  $('.message-submit').click(function() {
    insertMessage();
  });

  $(window).on('keydown', function(e) {
    if (e.which == 13) {
      insertMessage();
      return false;
    }
  })

  function insertResponseMessage(content) {
    $('<div class="message loading new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure><span></span></div>').appendTo($('.mCSB_container'));
    updateScrollbar();

    setTimeout(function() {
      $('.message.loading').remove();
      $('<div class="message new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure>' + content + '</div>').appendTo($('.mCSB_container')).addClass('new');
      setDate();
      updateScrollbar();
      i++;
    }, 500);
  }

});
