
const dp = (function () {
    const DEBUG = true;

    function logging(override) {
        return override === undefined ? DEBUG : override;
    }

    return {
        logging: logging
    }
}());

dp.log = function (tag, debug=undefined) {

    const override = debug;

    function _detail_log(stuff, title) {
        const start = title ? "<" + title + ">" : "<>";
        const end = title ? "</" + title + ">" : "</>";
        console.log(prefix() + start);
        console.log(stuff);
        console.log(prefix() + end);
    }

    function _short_log(stuff, title) {
        console.log(prefix() + (title ? title + " = " : "") + stuff);
    }

    function prefix() {
        return tag ? tag + " " : "";
    }

    function object(stuff) {
        return typeof stuff === 'object';
    }

    function log(stuff, title, detail) {
        if (dp.logging(override)) {
            if (detail === undefined) {
                detail = object(stuff);
            }
            if (detail) {
                _detail_log(stuff, title);
            } else {
                _short_log(stuff, title);
            }
        }
    }

    // detailed
    log.d = function (stuff, title) {
        return log(stuff, title, true);
    };

    // short
    log.s = function (stuff, title) {
        return log(stuff, title, true);
    }

    return log;
};

/**
 *
 * HTML attributes:
 *
 * ATTR                     DESC                    VALUE
 * data-io-prop             field name              url, user.email (`.` = split, `..` = `.`, dot used to escape)
 * data-io-type             field type              object, array, int, float, json
 * data-io-ignore           <ignore field>          <present>/<not-present>
 * data-io-template         <template elem>         <present>/<not-present>
 * data-io-array-container  <array item container>  <present>/<not-present>
 *
 * @type {val, set, get}
 */
dp.io = (function () {
    const log = dp.log("[io]");

    const val_funcs = (function () {

        function input_val($elem, value=undefined) {
            let input_type = $elem.attr('type');
            if (input_type === "checkbox") {
                return checkbox_val($elem, value);
            } else {
                return val($elem, value);
            }
        }

        function checkbox_val($elem, value=undefined) {
            if (value === undefined) {
                return $elem.is(':checked');
            } else {
                const modified = $elem.is(':checked') !== value;
                if (modified) $elem.attr("checked", "checked");
                return modified;
            }
        }

        function val($elem, value=undefined) {
            if (value === undefined) {
                return $elem.val();
            } else {
                const modified = $elem.val() !== value;
                if (modified) $elem.val(value);
                return modified;
            }
        }

        function text($elem, value=undefined) {
            if (value === undefined) {
                return $elem.text();
            } else {
                const modified = $elem.text() !== value;
                if (modified) $elem.text(value);
                return modified;
            }
        }

        return {
            input_val: input_val,
            val: val,
            text: text,
        }
    }());


    const identity = function (stuff) {
        return stuff;
    }

    const mapping = {
        textarea: val_funcs.val,
        input: val_funcs.input_val,
        select: val_funcs.val,
        span: val_funcs.text
    };

    const deserializers = {
        int: parseInt,
        float: parseFloat,
        json: JSON.parse
    };


    const serializers = {
        json: JSON.stringify
    };

    function serializer($elem) {
        const type = $elem.attr('data-io-type');
        return serializers.hasOwnProperty(type) ? serializers[type] : identity;
    }

    function deserializer($elem) {
        const type = $elem.attr('data-io-type');
        return deserializers.hasOwnProperty(type) ? deserializers[type] : identity;
    }

    function val($elem, data=null) {
        const tag = $elem.prop('tagName').toLowerCase();
        let valFunction;
        try {
            valFunction = mapping[tag];
        } catch (e) {
            valFunction = mapping.text;
            log("[ERROR] mapping function not present for tag = " + tag);
        }
        if (data == null) {
            const deserialize = deserializer($elem);
            return deserialize(valFunction($elem));
        } else {
            const serialize = serializer($elem);
            return valFunction($elem, serialize(data));
        }
    }

    function get($elem) {
        return val($elem);
    }

    function set($elem, value) {
        return val($elem, value);
    }

    const forms = (function () {

        function defaultTemplateFinder($elem) {
            return $elem.find('[data-io-template]').clone()
                .removeAttr('data-io-template').removeAttr('data-io-ignore');
        }

        function getObjectData($elem) {
            const data = {};
            dataChildren($elem).each(function (i, child) {
                const $child = $(child);
                data[$child.attr('data-io-prop')] = getData($child);
            });
            return data;
        }

        function getArrayData($elem) {
            const data = [];
            dataChildren($elem).each(function (i, child) {
                const $child = $(child);
                data.push(getData($child));
            });
            return data;
        }

        function getData($elem) {
            const type = $elem.attr('data-io-type');
            if (type === 'object') {
                return getObjectData($elem);
            } else if (type === 'array') {
                return getArrayData($elem);
            } else {
                return get($elem);
            }
        }

        function dataChildren($elem) {
            const level = parseInt($elem.attr('data-io-level')) + 1;
            return $elem.find('[data-io-prop][data-io-level="' + level + '"]:not([data-io-ignore],[data-io-template])')
        }

        function arrayContainer($elem) {
            return $elem.find('[data-io-array-container]') || $elem;
        }

        function setDataObject($elem, data) {
            let modified = false;
            dataChildren($elem).each(function (i, child) {
                const $child = $(child);
                let child_modified = setData($child, data[$child.attr('data-io-prop')]);
                modified = modified || child_modified;
            })
            return modified;
        }

        function setDataArray($elem, data, templateFinder) {
            const $children = equalizedArrayChildren($elem, data, templateFinder);

            let modified = false;

            if ($children) {
                $children.each(function (i, child) {
                    const $child = $(child);
                    let child_modified = setData($child, data[i]);
                    modified = modified || child_modified;
                });
            } else {
                log('No children returned!!!');
                log($elem, 'element');
                log(data, 'data');
                log(templateFinder, 'templateFinder');
            }
            return modified;
        }

        function setData($elem, data, templateFinder=undefined) {
            const type = $elem.attr('data-io-type');
            // log('setData() called...');
            // log($elem, '$elem');
            // log(data, 'data');
            // log(type, 'type');
            if (type === 'object') {
                return setDataObject($elem, data);
            } else if (type === 'array') {
                return setDataArray($elem, data, templateFinder);
            } else {
                return set($elem, data);
            }
        }
        
        function removeArrayChildren($elem, $children, data) {
            $children.slice(data.length, -1).each(function (i, child) {
                const $child = $(child);
                $child.remove();
            });
            return dataChildren($elem);
        }
        
        function addArrayChildren($elem, $children, data, templateFinder) {
            // log(data.length, 'data.length');
            // log($children.length, '$children.length');
            for (let i = $children.length; i < data.length; i++) {
                const $template = getArrayItemTemplate($elem, templateFinder);
                arrayContainer($elem).append($template);
            }
            return dataChildren($elem);
        }

        function equalizedArrayChildren($elem, data, templateFinder) {
            let $children = dataChildren($elem);
            if ($children.length > data.length) {
                return removeArrayChildren($children, data);
            } else if (data.length > $children.length) {
                return addArrayChildren($elem, $children, data, templateFinder);
            } else {  // data.length == $children.length
                return $children;
            }
        }

        function getArrayItemTemplate($container, templateFinder) {
            let $template = templateFinder ? templateFinder($container) : null;
            return $template || defaultTemplateFinder($container);
        }

        function encode(str) {
            return str.replaceAll(/(?<!\.)\./g, '..');  // replace n occurrences of `.` with n+1 occurrences
        }

        function decode(str) {
            return str.replaceAll(/(?<!\.)\.\./g, '.');  // replace n occurrences of `.` with n-1 occurrences
        }

        function toPath(field) {
            return field.split(/(?<!\.)\.(?!\.)/).map(decode);  // split by `.` but not `..` or `...`
        }

        function toField(path) {
            return path.map(encode).join('.');
        }

        return {
            getData: getData,
            setData: setData
        }
    }());

    return {
        forms: forms,
        val: val,
        get: get,
        set: set
    }
}());

/**
* toast or growl
*
* @type {e, d, s, long, short}
*/
dp.toast = (function() {
    const log = dp.log("[toast]");

    log('main.toast.init: should fire only once');

    const TOAST = {
        classes: {
            'error': 'error_toast',
            'success': 'success_toast'
        },
        durations: {
            'default': 2000,
            'short': 1000,
            'long': 5000
        }
    };

    function getToastElements() {
        return {
            $main: $('#toast_msg_element'),
            $msg: $('#toast_msg')
        };
    }

    /**
     *
     * @param msg
     * @param type {string} error|success|<default>
     * @param duration {string} long|short|<default>
     */
    function toast(msg, type, duration) {
        const class_to_add = TOAST.classes[type];
        const duration_ms = TOAST.durations[duration] || TOAST.durations.default;
        const toast_elements = getToastElements();

        toast_elements.$msg.html(msg);
        toast_elements.$main.removeClass(TOAST.classes.error);
        toast_elements.$main.removeClass(TOAST.classes.success);

        if (class_to_add) {
            toast_elements.$main.addClass(class_to_add);
        }
        toast_elements.$main.addClass('is_shown');

        setTimeout(function () {
            toast_elements.$main.removeClass('is_shown');
        }, duration_ms + 1000);
    }

    function error(msg, duration) {
        return toast(msg, 'error', duration);
    }
    function def(msg, duration) {
        return toast(msg, 'success', duration);
    }
    function success(msg, duration) {
        return toast(msg, 'success', duration);
    }
    function long(msg, type) {
        return toast(msg, type, 'long');
    }
    function short(msg, type) {
        return toast(msg, type, 'short');
    }

    return {
        e: error,
        d: def,
        s: success,
        long: long,
        short: short
    };

}());


/**
 * universal progress indicator
 *
 * @type {hide, showUpload, show, hideUpload}
 */
dp.progress = (function () {

    function showDefaultProgress(msg) {
        var $progress = $('#default_progress_element');
        var $msg = $('#default_progress_msg');

        $progress.addClass('is_shown');
        if (msg) {
            $msg.html(msg).removeClass('hidden');
        } else {
            $msg.html('').addClass('hidden');
        }
    }

    function hideDefaultProgress() {
        $('#default_progress_element').removeClass('is_shown');
    }

    function showUploadProgress(msg) {
        var $progress = $('#upload_progress_element');
        var $msg = $('#upload_progress_msg');

        $progress.addClass('is_shown');
        if (msg) {
            $msg.html(msg).removeClass('hidden');
        } else {
            $msg.html('').addClass('hidden');
        }
    }

    function hideUploadProgress() {
        $('#upload_progress_element').removeClass('is_shown');
    }

    return {
        show: showDefaultProgress,
        hide: hideDefaultProgress,
        showUpload: showUploadProgress,
        hideUpload: hideUploadProgress
    }
}());


dp.requests = (function () {
    const log = dp.log("[requests]");
    log('main.requests.init: should fire only once');

    function getCSRF() {
        return $('#csrf_token').val();
    }

    $.ajaxSetup({
        beforeSend: function(xhr, settings) {
            if (settings.type === 'POST' && !this.crossDomain) {
                xhr.setRequestHeader("X-CSRFToken", getCSRF());
            }
        }
    });

    function getUrlParams(url) {
        var params = {};
        var parser = document.createElement('a');
        parser.href = url;
        var query = parser.search.substring(1);
        var vars = query.split('&');
        for (var i = 0; i < vars.length; i++) {
            var pair = vars[i].split('=');
            params[pair[0]] = decodeURIComponent(pair[1]);
        }
        return params;
    }

    function getUrlParam(url, key) {
        return getUrlParams(url)[key];
    }


    function showProgressElem($progress) {
        // hack to disable progress all together
        if ($progress === false) {
            return;
        }
        if ($progress) {
            $progress.show();
        } else {
            dp.progress.show();
        }
    }

    function hideProgressElem($progress) {
        // hack to disable progress all together
        if ($progress === false) {
            return;
        }
        if ($progress) {
            $progress.hide();
        } else {
            dp.progress.hide();
        }
    }

    function disableButton($button) {
        if ($button) {
            $button.attr('disabled', 'disabled');
            $button.css({'cursor': 'wait'});
        }
    }

    function enableButton($button) {
        if ($button) {
            $button.removeAttr('disabled');
            $button.css({'cursor': 'pointer'});
        }
    }

    function doOnSuccess(data, onSuccess, mute_success) {
        if (data && data['msg'] && !mute_success) {
            dp.toast.s(data['msg'], 'short');
        }
        if (onSuccess) {
            onSuccess(data);
        }
    }

    function doOnError(response, onError, mute_error) {
        try {
            var data = JSON.parse(response.responseText);
            if (data && data['msg'] && !mute_error) {
                dp.toast.e(data['msg']);
            }
            if (onError) {
                onError(data);
            }
        } catch (e) {
            if (!mute_error) {
                dp.toast.e('Unexpected error');
            }
            log(e);
            if (onError) {
                onError({});
            }
        }
    }

    function makeGetUrl(url, data) {
        if (data) {
            var qs = $.param(data);
            if (qs) {
                url = url + '?' + qs;
            }
        }
        return url;
    }

    function makeHtmlGetRequest(url, data, $button, $progress, onSuccess, onError, mute_success, mute_error) {
        showProgressElem($progress);
        disableButton($button);

        url = makeGetUrl(url, data);
        data = null;

        return $.ajax({
            url: url,
            type: 'GET'
        }).always(function () {
            hideProgressElem($progress);
            enableButton($button);
        }).done(function (data) {
            doOnSuccess(data, onSuccess, mute_success);
        }).fail(function (response) {
            doOnError(response, onError, mute_error);
        });
    }

    function makeRequest(method, url, data, $button, $progress, onSuccess, onError, mute_success, mute_error) {
        showProgressElem($progress);
        disableButton($button);

        method = method.toUpperCase();
        if (method === 'GET') {
            url = makeGetUrl(url, data);
            data = null;
        } else if (method === 'POST') {
            data = JSON.stringify(data);
        } else {
            throw ('method "' + method + '" not supported');
        }

        return $.ajax({
            url: url,
            type: method,
            data: data,
            dataType: 'json',
            contentType: 'application/json; charset=utf-8'
        }).always(function () {
            hideProgressElem($progress);
            enableButton($button);
        }).done(function (data) {
            doOnSuccess(data, onSuccess, mute_success);
        }).fail(function (response) {
            doOnError(response, onError, mute_error);
        });
    }

    function makeGetRequest(url, data, $button, $progress, onSuccess, onError, mute_success, mute_error) {
        return makeRequest('GET', url, data, $button, $progress, onSuccess, onError, mute_success, mute_error);
    }

    function makePostRequest(url, data, $button, $progress, onSuccess, onError, mute_success, mute_error) {
        return makeRequest('POST', url, data, $button, $progress, onSuccess, onError, mute_success, mute_error);
    }

    function makeMultiPartRequest(url, data, $button, $progress, onSuccess, onError, onProgressUpdate,
                                  mute_success, mute_error) {
        showProgressElem($progress);
        disableButton($button);

        return $.ajax({
            // Your server script to process the upload
            url: url,
            type: 'POST',

            // Form data
            data: data,

            // Tell jQuery not to process data or worry about content-type
            // You *must* include these options!
            cache: false,
            contentType: false,
            processData: false,

            // Custom XMLHttpRequest
            xhr: function() {
                var myXhr = $.ajaxSettings.xhr();

                if (onProgressUpdate && myXhr.upload) {
                    // For handling the progress of the upload
                    myXhr.upload.addEventListener('progress', function(e) {
                        onProgressUpdate(e, $progress);
                    }, false);
                }

                return myXhr;
            }
        }).always(function () {
            hideProgressElem($progress);
            enableButton($button);
        }).done(function (data) {
            doOnSuccess(data, onSuccess, mute_success);
        }).fail(function (response) {
            doOnError(response, onError, mute_error);
        });

    }

    /**
     * container for dynamic content
     *
     * @returns {*}
     */
    function pageContainer() {
        return $('body');
    }

    /**
     * loading pages and re-writing urls using ajax
     *
     * Example (not in use right now) TODO: make this work
     *
     * @param url
     * @param data
     * @param $button
     * @param $progress
     * @returns {*}
     */
    function loadPage(url, data, $button, $progress){
        showProgressElem($progress);
        disableButton($button);

        data = data || {};
        data['format'] = 'partial-html';

        url = makeGetUrl(url, data);
        data = null;

        var xhr;

        function getXhr() {
            return xhr;
        }

        // escape should stop the operation
        $(document).on('keyup.replace_page', function(e) {
            if (e.keyCode === KEYS.escape) {
                var x = getXhr();
                if (x && x.abort) {
                    x.abort();
                    dp.toast.short('Canceled');
                    return false;
                }
            }
        });

        xhr = $.ajax({
            url: url,
            type: 'GET',
            data: data,
            contentType: 'text/html; charset=utf-8'
        }).always(function () {
            // turn our escape handler off
            $(document).off('keyup.replace_page');

            hideProgressElem($progress);
            enableButton($button);
        }).done(function (response) {
            pageContainer().html(response.html);
            document.title = response.pageTitle;
            window.history.pushState({html: response.html, pageTitle: response.pageTitle}, '', url);
        }).fail(function (response) {
            log(response);
            dp.toast.e('Error loading url "' + url + '"');
        });

        return xhr;

    }

    /**
     * setup for history to work with this
     */
    function setupLoadPage() {
        window.onpopstate = function(e){
            if(e.state){
                pageContainer().html(e.state.html);
                document.title = e.state.pageTitle;
            }
        };
    }

    return {
        html: makeHtmlGetRequest,
        get: makeGetRequest,
        post: makePostRequest,
        make: makeRequest,
        multiPart: makeMultiPartRequest,

        makeGetUrl: makeGetUrl,
        getUrlParams: getUrlParams,
        getUrlParam: getUrlParam,

        loadPage: loadPage,
        setupLoadPage: setupLoadPage
    }

}());