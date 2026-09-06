const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = "claude-sonnet-5";

async function callClaude(systemPrompt, userPrompt, maxTokens = 300) {
  if (!ANTHROPIC_API_KEY) {
    throw new Error("AI is not configured on this server yet — ask the admin to set ANTHROPIC_API_KEY.");
  }
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: MODEL,
      max_tokens: maxTokens,
      system: systemPrompt,
      messages: [{ role: "user", content: userPrompt }],
    }),
  });
  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`AI request failed (${res.status}): ${errText.slice(0, 200)}`);
  }
  const data = await res.json();
  return (data.content || []).map((b) => b.text || "").join("").trim();
}

async function translateText(text, targetLang) {
  const prompt = `Translate the following message to ${targetLang}. Output ONLY the translation, nothing else — no notes, no quotes.\n\nMessage: ${text}`;
  return callClaude("You are a precise, concise translator.", prompt, 300);
}

function transcript(messages) {
  return messages.map((m) => `${m.from}: ${m.text || "[photo]"}`).join("\n");
}

async function summarizeConversation(messages) {
  const prompt = `Summarize this conversation in 2-3 short, plain sentences. Focus on what was actually discussed or decided.\n\n${transcript(messages)}`;
  return callClaude("You summarize chat conversations concisely and neutrally.", prompt, 200);
}

async function suggestReplies(messages) {
  const prompt = `Based on this conversation, suggest 3 short, casual reply options (each under 8 words) for what the last speaker might want to say next. Reply with ONLY the 3 options, one per line, no numbering or extra text.\n\n${transcript(messages)}`;
  const result = await callClaude("You suggest short, casual, natural chat replies.", prompt, 150);
  return result
    .split("\n")
    .map((s) => s.replace(/^[-•\d.)\s]+/, "").trim())
    .filter(Boolean)
    .slice(0, 3);
}

module.exports = { translateText, summarizeConversation, suggestReplies };
