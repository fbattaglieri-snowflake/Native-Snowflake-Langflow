(function () {
  var reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  var $ = function (id) { return document.getElementById(id); };

  // Reveal on scroll, once.
  var items = document.querySelectorAll('.reveal');
  if ('IntersectionObserver' in window) {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); }
      });
    }, { threshold: 0.15 });
    items.forEach(function (el) { io.observe(el); });
  } else {
    items.forEach(function (el) { el.classList.add('in'); });
  }

  // Segmented controls.
  function segment(selector, onPick) {
    var buttons = document.querySelectorAll(selector);
    buttons.forEach(function (b) {
      b.addEventListener('click', function () {
        buttons.forEach(function (x) { x.setAttribute('aria-pressed', String(x === b)); });
        onPick(b);
      });
    });
  }

  // Home: two example flows that play in a loop.
  var win = $('heroWin');
  if (win) {
    var scenes = {
      agent: {
        title: 'Maintenance assistant · example flow', role: 'MAINTENANCE_ANALYST', ctx: '4 objects',
        steps: [['on', 'nIn', 0], ['go', 'w1'], ['on', 'nAif', 1], ['go', 'w2'], ['on', 'nAg', 2],
          ['tool', 't1', 'nAn'], ['tool', 't2', 'nSe'], ['tool', 't3', 'nMcp'],
          ['go', 'w3'], ['on', 'nDec', 3], ['go', 'wLow'], ['on', 'nCa', 4], ['go', 'w5'], ['on', 'nA2a'],
          ['go', 'w6'], ['on', 'nOut', 5], ['go', 'w7'], ['on', 'nMem']]
      },
      data: {
        title: 'Invoice loader · example flow', role: 'DATA_ENGINEER', ctx: '5 objects',
        steps: [['on', 'dTrig', 0], ['go', 'd1'], ['on', 'dAif', 1], ['go', 'd2'], ['on', 'dUdf'],
          ['tool', 'dr', 'dRef'], ['log', 2], ['go', 'd3'], ['on', 'dDec', 3],
          ['go', 'dPass'], ['on', 'dSql', 4], ['go', 'd5'], ['on', 'dLoad'],
          ['go', 'd6'], ['on', 'dSum'], ['go', 'd7'], ['on', 'dOut', 5]]
      }
    };
    var current = location.hash === '#data-engineering' ? 'data' : 'agent', runId = 0;
    var wait = function (ms) { return new Promise(function (r) { setTimeout(r, ms); }); };
    var show = function () {
      win.querySelectorAll('[data-scene]').forEach(function (el) {
        if (el.tagName === 'BUTTON') { el.setAttribute('aria-pressed', String(el.dataset.scene === current)); }
        else { el.classList.toggle('on', el.dataset.scene === current); }
      });
      $('heroTitle').textContent = scenes[current].title;
      $('heroRole').textContent = scenes[current].role;
      $('heroCtx').textContent = scenes[current].ctx;
    };
    var run = async function () {
      var id = ++runId, name = current;
      var svg = win.querySelector('svg[data-scene="' + name + '"]');
      var dot = svg.querySelector('.dot');
      var parts = svg.querySelectorAll('.node, .wire');
      var lines = win.querySelector('ol[data-scene="' + name + '"]').children;
      var ok = function () { return id === runId; };
      var setAll = function (on) {
        parts.forEach(function (el) { el.classList.toggle('on', on && !el.classList.contains('alt')); });
        Array.prototype.forEach.call(lines, function (li) { li.classList.toggle('on', on); });
      };
      var travel = function (wire, reverse) {
        var path = $(wire);
        return new Promise(function (resolve) {
          var len = path.getTotalLength(), dur = Math.max(260, Math.min(620, len * 2.2)), t0 = performance.now();
          path.classList.add('on');
          dot.style.opacity = 1;
          (function frame(now) {
            if (!ok()) { resolve(); return; }
            var p = Math.min(1, (now - t0) / dur);
            var pt = path.getPointAtLength((reverse ? 1 - p : p) * len);
            dot.setAttribute('cx', pt.x); dot.setAttribute('cy', pt.y);
            if (p < 1) { requestAnimationFrame(frame); } else { resolve(); }
          })(t0);
        });
      };
      if (reduce) { setAll(true); return; }
      while (ok()) {
        setAll(false); dot.style.opacity = 0;
        await wait(500);
        for (var i = 0; i < scenes[name].steps.length && ok(); i++) {
          var s = scenes[name].steps[i];
          if (s[0] === 'on') {
            $(s[1]).classList.add('on');
            if (s[2] !== undefined) { lines[s[2]].classList.add('on'); }
            await wait(200);
          } else if (s[0] === 'log') {
            lines[s[1]].classList.add('on');
          } else if (s[0] === 'go') {
            await travel(s[1], false);
          } else {
            await travel(s[1], true);
            if (!ok()) { break; }
            $(s[2]).classList.add('on');
            await wait(160);
            await travel(s[1], false);
          }
        }
        if (!ok()) { return; }
        dot.style.opacity = 0;
        await wait(2800);
      }
    };
    show();
    requestAnimationFrame(function () {
      win.classList.add('ready');
      setTimeout(run, reduce ? 0 : 1400);
    });
    win.querySelectorAll('button[data-scene]').forEach(function (b) {
      b.addEventListener('click', function () { current = b.dataset.scene; show(); run(); });
    });
    $('rerun').addEventListener('click', run);
  }

  // Home: role demo.
  var result = $('govResult');
  if (result) {
    segment('[data-role]', function (b) {
      var deny = b.dataset.role === 'deny';
      result.classList.toggle('deny', deny);
      result.innerHTML = deny
        ? '<b>AUTHZ_ROLE_DENIED</b>The active role lacks the privilege. The node stops here and no other role is tried.'
        : '<b>Rows returned</b>Statement ran under MAINTENANCE_ANALYST. Run, spans and audit event recorded.';
    });
  }

  // Models: the two proxy editions.
  var up = $('upName');
  if (up) {
    var editions = {
      rest: {
        name: 'Cortex REST API', path: '/api/v2/cortex/v1',
        text: 'Requests go straight from the proxy to the Cortex REST API.',
        repo: 'snowflake-proxy-cortexrestapi'
      },
      gateway: {
        name: 'Cortex AI Gateway', path: '/api/v2/aigateways/<gateway>/v1',
        text: 'Requests go through Cortex AI Gateway. On top of the same proxy you get traces you can query, usage history per user, token quotas and a model allowlist.',
        repo: 'snowflake-proxy-cortex-aigateway'
      }
    };
    segment('[data-ed]', function (b) {
      var e = editions[b.dataset.ed];
      up.textContent = e.name;
      $('upPath').textContent = e.path;
      $('edText').textContent = e.text;
      $('edRepo').textContent = e.repo;
    });
  }

  // Examples: tabs.
  var tabs = document.querySelectorAll('[role=tab]');
  tabs.forEach(function (tab) {
    tab.addEventListener('click', function () {
      tabs.forEach(function (t) {
        var sel = t === tab;
        t.setAttribute('aria-selected', String(sel));
        $(t.getAttribute('aria-controls')).classList.toggle('on', sel);
      });
    });
  });
})();
