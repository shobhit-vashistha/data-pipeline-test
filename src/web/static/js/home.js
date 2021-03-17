// depends on dp.js

dp.home = (function () {
    const log = dp.log('[home]');

    const example_impression_event_data = {
      "eid": "IMPRESSION",
      "ets": 1615468479199,
      "ver": "3.0",
      "mid": "IMPRESSION:1c2bacd9e1e04f7a0c4ca3f7aa59e5ae",
      "actor": {
        "id": "d56fec8d-a814-436b-90c5-f396fc4dac42",
        "type": "User"
      },
      "context": {
        "channel": "igot",
        "pdata": {
          "id": "lex-web-ui",
          "ver": "1.0.0",
          "pid": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"
        },
        "env": "prod",
        "sid": "",
        "did": "2c90e550e243f6285c0955b1e70d1681",
        "cdata": [],
        "rollup": {}
      },
      "object": {
        "ver": "1.0",
        "id": "page/learn",
        "pageid": "page/learn",
        "type": "",
        "uri": "page/learn"
      },
      "tags": [],
      "edata": {
        "pageid": "page/learn",
        "pageUrl": "page/learn",
        "pageUrlParts": [
          "page",
          "learn"
        ],
        "refferUrl": "page/home",
        "objectId": null
      }
    };

    const example_batch_data = {
        "id": "api.sunbird.telemetry",
        "ver": "3.0",
        "params": {
            "msgid": "bc73d44e53ae52aeb58ac84a23ab888a"
        },
        "ets": 1615468495474,
        "events": [
            example_impression_event_data
        ]
    }

    const default_test_case_data = {
        'name': 'untitled',
        'consumer_timeout': 20,
        'data': [example_batch_data],
        'producers': [
            {
                'producer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.ingest'
            }
        ],
        'consumers': [
            {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.ingest',
                'capture': true,
                'expected_count': 1,
            }, {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.raw',
                'capture': true,
                'expected_count': 1,
            }, {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.unique',
                'capture': true,
                'expected_count': 1,
            }, {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.denorm',
                'capture': true,
                'expected_count': 1,
            }, {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.druid.events.telemetry',
                'capture': true,
                'expected_count': 1,
            }, {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.failed',
                'capture': true,
                'expected_count': 0,
            }, {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.extractor.failed',
                'capture': true,
                'expected_count': 0,
            }, {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.duplicate',
                'capture': true,
                'expected_count': 0,
            }, {
                'consumer_type': 'kafka',
                'kafka_topic': 'dev.telemetry.extractor.duplicate',
                'capture': true,
                'expected_count': 0,
            }
        ]
    };


    function getTestCaseFormData($body) {
        const $root = $body.find('.test_case_data');
        return dp.io.forms.getData($root);
    }

    function setTestCaseData($body, data) {
        const $root = $body.find('.test_case_data');
        log(data);
        return dp.io.forms.setData($root, data);
    }

    function setupSubmitBtn($body) {
        $('.btn-submit-test').on('click', function (e) {
            e.stopPropagation();
            e.preventDefault();
            const test_data = getTestCaseFormData($body);
            console.log(test_data);
            let onSuccess = function (data) {
                log(data, 'success');
            };
            let onError = function (data) {
                log(data, 'error');
            };
            dp.requests.post('/test/', test_data, $(this), null, onSuccess, onError);

            return false;
        });
    }
    
    function setupCaptureCheckbox($body) {
        $body.on('change', '.tc_consumer_capture', function () {
            const $check = $(this);
            if (this.checked) {
                $check.closest(".tc_consumer").removeClass('tc_disabled_consumer').removeAttr('data-io-ignore');
            } else {
                $check.closest(".tc_consumer").addClass('tc_disabled_consumer').attr('data-io-ignore', '');
            }
        });
    }

    function setupInitTestCaseData($body) {
        setTestCaseData($body, default_test_case_data);
    }

    function init($body) {
        setupInitTestCaseData($body);
        setupCaptureCheckbox($body);
        setupSubmitBtn($body);
    }

    return {
        init: init
    };
}());

dp.pg = (function () {

    const open_topics = {};
    const topic_callbacks = {};

    let poll_running = false;

    function runningTopics() {
        const topics = [];
        $.each(open_topics, function (topic, status) {
            if (status === 'running') {
                topics.push(topic);
            }
        });
        return topics;
    }

    function poll(running_topics) {
        $.ajax({
            type: 'POST',
            url: '/poll/',
            data: JSON.stringify(running_topics),
            contentType: "application/json",
            dataType: 'json'
        }).success(function (data) {
            processData(data);
        }).fail(function (data) {

        }).send();
    }

    function processData(data) {
        $.each(data, function (topic, messages) {
            const callback = topic_callbacks[topic];
            if (callback) {
                $.each(messages, function (i, message) {
                    callback(message);
                });
            }
        });
    }

    function runPolling() {
        setInterval(function () {
            if (poll_running) {
                const running_topics = runningTopics();
                if (running_topics.length > 0) {
                    poll(running_topics);
                }
            }
        }, 2000);
    }

    function startPolling() {
        poll_running = true;
    }

    function stopPolling() {
        poll_running = false;
    }

    function startKafkaConsumer(topic, onReceive) {
        topic_callbacks[topic] = onReceive;
        open_topics[topic] = 'running';
        
    }

    function notify($notification_area, msg, error=false) {
        const $notification = $notification_area.find('.notification');
        $notification.text(msg);
        if (error) {
            $notification.addClass('error');
        } else {
            $notification.removeClass('error');
        }
        $notification_area.removeClass('hidden');

        setTimeout(function () {
            $notification_area.addClass('hidden');
        }, 3000);
    }

    function setupConsumerAdd() {
        $('.add_consumer_btn').on('click', function () {
            const $this = $(this);
            $this.attr('disabled', 'disabled');
            const $form = $this.closest('.add-consumer-section');
            const $section = $this.closest('.consumer-messages');
            const $notification_area = $form.find('.notification-area');
            const topic = $this.closest('.add-consumer-section').find('.data-topic').val();
            if (open_topics.hasOwnProperty(topic)) {
                notify($notification_area, 'topic already present', true);
                return;
            }
            open_topics[topic] = 'opening';
            const $nav_item_template = $form.find('.nav-template .nav-item');
            const $nav_item_container = $section.find('.consumers-nav');

            const $tab_template = $section.find('.consumer-tab-template .consumer');
            const $tab_container = $section.find('.consumers-tab-container');

            const $msg_template = $section.find('.message-template .message');

            const topic_id = topic.replace('.', '_');

            $nav_item_container.find('.nav-link.active').removeClass('active');
            const $new_nav_item = $nav_item_template.clone();
            const $new_nav_item_msg_count = $new_nav_item.find('.msg-count');
            $new_nav_item.find('.data-topic').text(topic).attr('href', '#' + topic_id);
            $nav_item_container.append($new_nav_item);

            $tab_container.find('.consumer.active').removeClass('active');
            const $new_tab = $tab_template.clone();
            const $messages_container = $new_tab.find('.messages');
            $new_tab.attr('id', '#' + topic_id).find('.topic').text(topic);
            $tab_container.append($new_tab);

            startKafkaConsumer(topic, function(msg) {
                $messages_container.append(
                    $msg_template.clone().text(msg)
                );
                $new_nav_item_msg_count.text(
                    (parseInt($new_nav_item_msg_count.text()) + 1) + ""
                );
            });
            $this.removeAttr('disabled');
        });
    }

    function init() {

    }

    return {
        init: init,
        open_topics: open_topics
    }
}());


$(document).ready(function() {
    const $body = $('body');
    dp.home.init($body);
});
