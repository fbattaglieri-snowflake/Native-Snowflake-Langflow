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

  // Home: one run of the flow, replayable.
  var win = $('heroWin');
  if (win) {
    var dot = $('dot'), logItems = $('log').children, runId = 0;
    var all = win.querySelectorAll('.node, .wire');
    var wait = function (ms) { return new Promise(function (r) { setTimeout(r, ms); }); };
    var travel = function (path, reverse, id) {
      return new Promise(function (resolve) {
        var len = path.getTotalLength(), dur = Math.max(260, Math.min(620, len * 2.2)), t0 = performance.now();
        path.classList.add('on');
        dot.style.opacity = 1;
        (function frame(now) {
          if (id !== runId) { resolve(); return; }
          var p = Math.min(1, (now - t0) / dur);
          var pt = path.getPointAtLength((reverse ? 1 - p : p) * len);
          dot.setAttribute('cx', pt.x); dot.setAttribute('cy', pt.y);
          if (p < 1) { requestAnimationFrame(frame); } else { resolve(); }
        })(t0);
      });
    };
    var setAll = function (on) {
      all.forEach(function (el) { el.classList.toggle('on', on); });
      Array.prototype.forEach.call(logItems, function (li) { li.classList.toggle('on', on); });
    };
    var run = async function () {
      var id = ++runId;
      setAll(false); dot.style.opacity = 0;
      if (reduce) { setAll(true); $('wHigh').classList.remove('on'); return; }
      var ok = function () { return id === runId; };
      var on = function (n) { $(n).classList.add('on'); };
      var log = function (i) { logItems[i].classList.add('on'); };
      var go = async function (w, reverse) { await travel($(w), reverse, id); return ok(); };
      var tool = async function (w, n) {
        if (!(await go(w, true))) return false;
        on(n); await wait(160);
        return go(w, false);
      };
      await wait(300); if (!ok()) return;
      on('nIn'); log(0);
      if (!(await go('w1'))) return; on('nAif'); log(1); await wait(200);
      if (!(await go('w2'))) return; on('nAg'); log(2);
      if (!(await tool('t1', 'nAn'))) return;
      if (!(await tool('t2', 'nSe'))) return;
      if (!(await tool('t3', 'nMcp'))) return;
      if (!(await go('w3'))) return; on('nDec'); log(3); await wait(300);
      if (!(await go('wLow'))) return; on('nCa'); log(4); await wait(200);
      if (!(await go('w5'))) return; on('nA2a'); await wait(200);
      if (!(await go('w6'))) return; on('nOut'); log(5);
      if (!(await go('w7'))) return; on('nMem');
      dot.style.opacity = 0;
    };
    requestAnimationFrame(function () {
      win.classList.add('ready');
      setTimeout(run, reduce ? 0 : 1500);
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
