const elements = {
  connectionLabel: document.querySelector('#connection-label'),
  statusDot: document.querySelector('#status-dot'),
  sampleTime: document.querySelector('#sample-time'),
  liveBrokerCount: document.querySelector('#live-broker-count'),
  brokerTotal: document.querySelector('#broker-total'),
  topicCount: document.querySelector('#topic-count'),
  partitionCount: document.querySelector('#partition-count'),
  replicaHealth: document.querySelector('#replica-health'),
  groupCount: document.querySelector('#group-count'),
  groupMembers: document.querySelector('#group-members'),
  topicsBody: document.querySelector('#topics-body'),
  brokersList: document.querySelector('#brokers-list'),
  partitionsBody: document.querySelector('#partitions-body'),
  groupsBody: document.querySelector('#groups-body'),
  offsetsBody: document.querySelector('#offsets-body'),
  groupTopic: document.querySelector('#group-topic'),
  groupAssignment: document.querySelector('#group-assignment'),
  groupSession: document.querySelector('#group-session'),
  groupMessages: document.querySelector('#group-message-list'),
  publishTopic: document.querySelector('#publish-topic'),
  fetchTopic: document.querySelector('#fetch-topic'),
  publishPartition: document.querySelector('#publish-partition'),
  fetchPartition: document.querySelector('#fetch-partition'),
  fetchOffset: document.querySelector('#fetch-offset'),
  messageList: document.querySelector('#message-list'),
  fetchState: document.querySelector('#fetch-state'),
  publishResult: document.querySelector('#publish-result'),
  toast: document.querySelector('#toast'),
};

let latestOverview = null;
let refreshInProgress = false;
let toastTimer = null;
let activeGroup = null;
let pendingGroupOffsets = [];
let groupHeartbeatTimer = null;

const escapeHtml = (value) => String(value ?? '').replace(/[&<>"']/g, (character) => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
}[character]));

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
  });
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`);
  return body;
}

function emptyRow(columns, text) {
  return `<tr><td class="empty-state" colspan="${columns}">${escapeHtml(text)}</td></tr>`;
}

function renderOverview(overview) {
  latestOverview = overview;
  const liveBrokers = overview.brokers.filter((broker) => broker.alive);
  const healthyReplicas = overview.partitions.filter((partition) => partition.isr.length === partition.replicas.length).length;
  const activeMembers = overview.consumerGroups.reduce((total, group) => total + group.memberCount, 0);

  elements.liveBrokerCount.textContent = liveBrokers.length;
  elements.brokerTotal.textContent = `${overview.brokers.length} registered`;
  elements.topicCount.textContent = overview.topics.length;
  elements.partitionCount.textContent = overview.partitions.length;
  elements.replicaHealth.textContent = `${healthyReplicas} / ${overview.partitions.length} fully in sync`;
  elements.groupCount.textContent = overview.consumerGroups.length;
  elements.groupMembers.textContent = `${activeMembers} active members`;
  elements.sampleTime.textContent = `SAMPLED ${new Date(overview.sampledAtMs).toLocaleTimeString()}`;
  elements.connectionLabel.textContent = 'Coordinator online';
  elements.statusDot.classList.add('is-online');

  renderTopics(overview.topics);
  renderBrokers(overview.brokers);
  renderPartitions(overview.partitions);
  renderGroups(overview.consumerGroups);
  renderOffsets(overview.committedOffsets);
  syncTopicOptions(overview.topics);
}

function renderTopics(topics) {
  elements.topicsBody.innerHTML = topics.length ? topics.map((topic) => `
    <tr data-topic="${escapeHtml(topic.name)}">
      <td><button class="table-link topic-select" type="button" data-topic="${escapeHtml(topic.name)}">${escapeHtml(topic.name)}</button></td>
      <td>${topic.partitionCount}</td>
      <td><span class="number-cell">${topic.replicationFactor}×</span></td>
      <td>${topic.retentionMs > 0 ? `${Math.round(topic.retentionMs / 86400000)} d` : 'Disabled'}</td>
    </tr>`).join('') : emptyRow(4, 'No topics created');
  elements.topicsBody.querySelectorAll('.topic-select').forEach((button) => {
    button.addEventListener('click', () => selectTopic(button.dataset.topic));
  });
}

function renderBrokers(brokers) {
  elements.brokersList.innerHTML = brokers.length ? brokers.map((broker) => `
    <div class="broker-row">
      <span class="broker-led ${broker.alive ? 'is-online' : 'is-offline'}"></span>
      <div class="broker-details"><strong>${escapeHtml(broker.brokerId)}</strong><span>${escapeHtml(broker.host)}:${broker.port}</span></div>
      <span class="broker-heartbeat ${broker.alive ? '' : 'is-offline-text'}">${broker.alive ? `${Math.max(0, Math.floor((Date.now() - broker.lastHeartbeatMs) / 1000))}s ago` : 'OFFLINE'}</span>
    </div>`).join('') : '<p class="empty-state">No brokers registered</p>';
}

function renderPartitions(partitions) {
  elements.partitionsBody.innerHTML = partitions.length ? partitions.map((partition) => {
    const ratio = partition.replicas.length ? Math.round(partition.isr.length / partition.replicas.length * 100) : 0;
    const status = ratio === 100 ? 'healthy' : ratio > 0 ? 'degraded' : 'unavailable';
    return `<tr>
      <td><strong>${escapeHtml(partition.topic)}</strong><span class="subcell">P${partition.partitionId}</span></td>
      <td>${partition.leaderBrokerId ? escapeHtml(partition.leaderBrokerId) : '<span class="danger-text">none</span>'}</td>
      <td>${partition.replicas.length}</td>
      <td>${partition.isr.length} / ${partition.replicas.length}</td>
      <td class="mono">${partition.logStartOffset}</td>
      <td class="mono">${partition.highWatermark}</td>
      <td class="mono">${partition.logEndOffset}</td>
      <td><span class="replication-state ${status}"><i style="--health:${ratio}%"></i>${ratio}%</span></td>
    </tr>`;
  }).join('') : emptyRow(8, 'No partition metadata');
}

function renderGroups(groups) {
  elements.groupsBody.innerHTML = groups.length ? groups.map((group) => `
    <tr><td>${escapeHtml(group.groupId)}</td><td>${group.memberCount}</td><td class="mono">${group.generationId}</td><td>${group.assignedPartitionCount}</td></tr>`).join('') : emptyRow(4, 'No active groups');
}

function renderOffsets(offsets) {
  elements.offsetsBody.innerHTML = offsets.length ? offsets.slice().reverse().map((offset) => `
    <tr><td>${escapeHtml(offset.groupId)}</td><td>${escapeHtml(offset.topic)} <span class="subcell">P${offset.partition}</span></td><td class="mono">${offset.offset}</td></tr>`).join('') : emptyRow(3, 'No commits yet');
}

function syncTopicOptions(topics) {
  const previousPublish = elements.publishTopic.value;
  const previousFetch = elements.fetchTopic.value;
  const previousGroupTopic = elements.groupTopic.value;
  const options = '<option value="">Select topic</option>' + topics.map((topic) =>
    `<option value="${escapeHtml(topic.name)}">${escapeHtml(topic.name)}</option>`).join('');
  elements.publishTopic.innerHTML = options;
  elements.fetchTopic.innerHTML = options;
  elements.groupTopic.innerHTML = options;
  if (topics.some((topic) => topic.name === previousPublish)) elements.publishTopic.value = previousPublish;
  if (topics.some((topic) => topic.name === previousFetch)) elements.fetchTopic.value = previousFetch;
  if (topics.some((topic) => topic.name === previousGroupTopic)) elements.groupTopic.value = previousGroupTopic;
}

function selectTopic(topic) {
  elements.publishTopic.value = topic;
  elements.fetchTopic.value = topic;
  elements.publishPartition.value = '0';
  elements.fetchPartition.value = '0';
  elements.fetchOffset.value = '0';
  fetchMessages();
}

async function refreshOverview() {
  if (refreshInProgress) return;
  refreshInProgress = true;
  try {
    renderOverview(await api('/api/overview'));
  } catch (error) {
    elements.connectionLabel.textContent = 'Coordinator unavailable';
    elements.statusDot.classList.remove('is-online');
    elements.sampleTime.textContent = 'CONNECTION FAILED';
    showToast(error.message, true);
  } finally {
    refreshInProgress = false;
  }
}

async function fetchMessages() {
  const topic = elements.fetchTopic.value;
  if (!topic) {
    elements.fetchState.textContent = 'Choose a topic and fetch a partition.';
    return;
  }
  const query = new URLSearchParams({
    topic,
    partition: elements.fetchPartition.value || '0',
    offset: elements.fetchOffset.value || '0',
    limit: '50',
  });
  elements.fetchState.textContent = 'Fetching committed records…';
  try {
    const page = await api(`/api/messages?${query}`);
    elements.fetchOffset.value = page.nextOffset;
    elements.fetchState.textContent = `P${page.partition} · high-watermark ${page.highWatermark} · next offset ${page.nextOffset}`;
    elements.messageList.innerHTML = page.messages.length ? page.messages.map((message) => `
      <article class="message-row">
        <div class="message-meta"><span class="message-offset">${message.offset}</span><span class="message-key">${escapeHtml(message.key || '(no key)')}</span><time>${new Date(message.timestamp).toLocaleTimeString()}</time></div>
        <pre>${escapeHtml(message.payload)}</pre>
        <div class="message-foot"><span>${message.size} bytes</span><span class="mono">${escapeHtml(message.messageId.slice(0, 12))}</span></div>
      </article>`).join('') : '<p class="empty-state">No committed records at this offset</p>';
  } catch (error) {
    elements.fetchState.textContent = error.message;
    elements.messageList.innerHTML = '<p class="empty-state">Fetch failed</p>';
  }
}

function groupRequestBody() {
  return { groupId: activeGroup.groupId, consumerId: activeGroup.consumerId };
}

function showGroupAssignments(assignments) {
  elements.groupAssignment.textContent = assignments.length
    ? assignments.map((assignment) => `${assignment.topic}/P${assignment.partition}`).join(' · ')
    : 'No partitions assigned';
}

async function heartbeatActiveGroup() {
  if (!activeGroup) return;
  try {
    const member = await api('/api/groups/heartbeat', {
      method: 'POST', body: JSON.stringify(groupRequestBody()),
    });
    showGroupAssignments(member.assignedPartitions);
  } catch (error) {
    elements.groupAssignment.textContent = error.message;
  }
}

async function pollGroup() {
  if (!activeGroup) return;
  elements.groupMessages.innerHTML = '<p class="empty-state">Polling assigned partitions…</p>';
  try {
    const result = await api('/api/groups/poll', {
      method: 'POST', body: JSON.stringify(groupRequestBody()),
    });
    showGroupAssignments(result.assignedPartitions);
    pendingGroupOffsets = result.nextOffsets;
    document.querySelector('#commit-group').disabled = pendingGroupOffsets.length === 0;
    elements.groupMessages.innerHTML = result.messages.length ? result.messages.map((message) => `
      <article class="message-row">
        <div class="message-meta"><span class="message-offset">${message.offset}</span><span class="message-key">${escapeHtml(message.topic)} / P${message.partition} · ${escapeHtml(message.key || '(no key)')}</span></div>
        <pre>${escapeHtml(message.payload)}</pre>
      </article>`).join('') : '<p class="empty-state">No committed records in assigned partitions</p>';
    showToast(`Fetched ${result.messages.length} record${result.messages.length === 1 ? '' : 's'}`);
  } catch (error) {
    pendingGroupOffsets = [];
    document.querySelector('#commit-group').disabled = true;
    elements.groupMessages.innerHTML = `<p class="empty-state">${escapeHtml(error.message)}</p>`;
    showToast(error.message, true);
  }
}

async function commitGroupOffsets() {
  if (!activeGroup || pendingGroupOffsets.length === 0) return;
  try {
    for (const position of pendingGroupOffsets) {
      await api('/api/groups/commit', {
        method: 'POST',
        body: JSON.stringify({ ...groupRequestBody(), ...position }),
      });
    }
    const committedCount = pendingGroupOffsets.length;
    pendingGroupOffsets = [];
    document.querySelector('#commit-group').disabled = true;
    showToast(`Committed ${committedCount} partition offset${committedCount === 1 ? '' : 's'}`);
    await refreshOverview();
  } catch (error) {
    showToast(error.message, true);
  }
}

async function leaveGroup() {
  if (!activeGroup) return;
  try {
    await api('/api/groups/leave', {
      method: 'POST', body: JSON.stringify(groupRequestBody()),
    });
  } catch (error) {
    showToast(error.message, true);
  }
  clearInterval(groupHeartbeatTimer);
  groupHeartbeatTimer = null;
  activeGroup = null;
  pendingGroupOffsets = [];
  elements.groupSession.classList.add('hidden');
  document.querySelector('#group-form').classList.remove('hidden');
  document.querySelector('#commit-group').disabled = true;
  await refreshOverview();
}

function showToast(message, isError = false) {
  clearTimeout(toastTimer);
  elements.toast.textContent = message;
  elements.toast.classList.toggle('is-error', isError);
  elements.toast.classList.add('is-visible');
  toastTimer = setTimeout(() => elements.toast.classList.remove('is-visible'), 3500);
}

document.querySelector('#refresh-button').addEventListener('click', refreshOverview);
document.querySelector('#fetch-button').addEventListener('click', fetchMessages);
document.querySelector('#fetch-form').addEventListener('submit', (event) => {
  event.preventDefault();
  fetchMessages();
});
document.querySelector('#open-topic-form').addEventListener('click', () => {
  document.querySelector('#topic-form').classList.toggle('hidden');
  document.querySelector('#topic-form input[name="topicName"]').focus();
});
document.querySelector('#open-group-form').addEventListener('click', () => {
  document.querySelector('#group-form').classList.toggle('hidden');
  document.querySelector('#group-form input[name="groupId"]').focus();
});
document.querySelector('#group-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const groupForm = event.currentTarget;
  const form = new FormData(groupForm);
  const request = {
    groupId: form.get('groupId'),
    consumerId: form.get('consumerId'),
    topic: form.get('topic'),
  };
  try {
    const result = await api('/api/groups/join', { method: 'POST', body: JSON.stringify(request) });
    activeGroup = { groupId: result.groupId, consumerId: result.consumerId };
    pendingGroupOffsets = [];
    groupForm.classList.add('hidden');
    elements.groupSession.classList.remove('hidden');
    showGroupAssignments(result.assignedPartitions);
    elements.groupMessages.innerHTML = '<p class="empty-state">Poll assigned partitions to read messages.</p>';
    document.querySelector('#commit-group').disabled = true;
    clearInterval(groupHeartbeatTimer);
    groupHeartbeatTimer = setInterval(heartbeatActiveGroup, 5000);
    await refreshOverview();
    showToast(`Joined ${result.groupId}`);
  } catch (error) {
    showToast(error.message, true);
  }
});
document.querySelector('#poll-group').addEventListener('click', pollGroup);
document.querySelector('#commit-group').addEventListener('click', commitGroupOffsets);
document.querySelector('#leave-group').addEventListener('click', leaveGroup);
document.querySelector('#topic-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const topicForm = event.currentTarget;
  const form = new FormData(topicForm);
  const payload = {
    topicName: form.get('topicName'),
    partitionCount: Number(form.get('partitionCount')),
    replicationFactor: Number(form.get('replicationFactor')),
    retentionMs: Number(form.get('retentionDays')) * 86400000,
    segmentSizeBytes: 67108864,
  };
  try {
    await api('/api/topics', { method: 'POST', body: JSON.stringify(payload) });
    topicForm.reset();
    topicForm.classList.add('hidden');
    showToast(`Topic ${payload.topicName} created`);
    await refreshOverview();
    selectTopic(payload.topicName);
  } catch (error) {
    showToast(error.message, true);
  }
});
document.querySelector('#publish-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  const payload = {
    topic: form.get('topic'),
    partition: Number(form.get('partition')),
    key: form.get('key'),
    payload: form.get('payload'),
    ackMode: form.get('ackMode'),
  };
  try {
    const receipt = await api('/api/messages', { method: 'POST', body: JSON.stringify(payload) });
    elements.publishResult.textContent = `${receipt.topic} / P${receipt.partition} · offset ${receipt.baseOffset} · ${receipt.ackMode}`;
    elements.fetchTopic.value = receipt.topic;
    elements.fetchPartition.value = receipt.partition;
    elements.fetchOffset.value = receipt.baseOffset;
    await refreshOverview();
    await fetchMessages();
    showToast('Record published');
  } catch (error) {
    elements.publishResult.textContent = error.message;
    showToast(error.message, true);
  }
});

elements.fetchTopic.addEventListener('change', () => {
  elements.fetchPartition.value = '0';
  elements.fetchOffset.value = '0';
});

refreshOverview();
setInterval(refreshOverview, 5000);
