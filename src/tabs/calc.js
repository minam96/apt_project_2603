import { formatPrice } from '../utils.js';

export function calcLoan() {
  const P =
    parseFloat(document.getElementById("loanPrincipal").value) || 0;
  const r =
    parseFloat(document.getElementById("loanRate").value) / 100 / 12;
  const n = parseInt(document.getElementById("loanYears").value) * 12;
  if (P <= 0 || r <= 0 || n <= 0) return;
  const monthly = (P * r * Math.pow(1 + r, n)) / (Math.pow(1 + r, n) - 1);
  const totalPay = monthly * n;
  const totalInt = totalPay - P;
  document.getElementById("loanResult").innerHTML = `
    <div class="calc-result-row"><span>월 상환액</span><span class="calc-result-val">${formatPrice(Math.round(monthly))}</span></div>
    <div class="calc-result-row"><span>총 상환액</span><span class="calc-result-val">${formatPrice(Math.round(totalPay))}</span></div>
    <div class="calc-result-row"><span>총 이자</span><span class="calc-result-val" style="color:var(--red)">${formatPrice(Math.round(totalInt))}</span></div>
    <div class="calc-result-row"><span>원금 대비 이자 비율</span><span class="calc-result-val">${((totalInt / P) * 100).toFixed(1)}%</span></div>
  `;
}

export function calcInvest() {
  const init =
    parseFloat(document.getElementById("investInit").value) || 0;
  const monthly =
    parseFloat(document.getElementById("investMonthly").value) || 0;
  const rate =
    parseFloat(document.getElementById("investRate").value) / 100;
  const years =
    parseInt(document.getElementById("investYears").value) || 0;
  const mr = rate / 12;
  const n = years * 12;
  let total = init;
  for (let i = 0; i < n; i++) {
    total = total * (1 + mr) + monthly;
  }
  const contributed = init + monthly * n;
  const gain = total - contributed;
  document.getElementById("investResult").innerHTML = `
    <div class="calc-result-row"><span>최종 자산</span><span class="calc-result-val">${formatPrice(Math.round(total))}</span></div>
    <div class="calc-result-row"><span>총 투자금</span><span class="calc-result-val">${formatPrice(Math.round(contributed))}</span></div>
    <div class="calc-result-row"><span>투자 수익</span><span class="calc-result-val" style="color:var(--green)">${formatPrice(Math.round(gain))}</span></div>
    <div class="calc-result-row"><span>수익률</span><span class="calc-result-val">${((gain / contributed) * 100).toFixed(1)}%</span></div>
  `;
}

export function calcCashflow() {
  const income =
    parseFloat(document.getElementById("cfIncome").value) || 0;
  const loan = parseFloat(document.getElementById("cfLoan").value) || 0;
  const livingInput =
    parseFloat(document.getElementById("cfLiving").value) || 0;
  const living = livingInput > 0 ? livingInput : Math.round(income * 0.4);
  const other = parseFloat(document.getElementById("cfOther").value) || 0;
  const cashflow = income - loan - living - other;
  const color = cashflow >= 0 ? "var(--green)" : "var(--red)";
  document.getElementById("cfResult").innerHTML = `
    <div class="calc-result-row"><span>월 소득</span><span class="calc-result-val">${formatPrice(income)}</span></div>
    <div class="calc-result-row"><span>월 대출상환</span><span class="calc-result-val">-${formatPrice(loan)}</span></div>
    <div class="calc-result-row"><span>월 생활비${livingInput === 0 ? " (소득 40%)" : ""}</span><span class="calc-result-val">-${formatPrice(living)}</span></div>
    ${other > 0 ? `<div class="calc-result-row"><span>기타 지출</span><span class="calc-result-val">-${formatPrice(other)}</span></div>` : ""}
    <div class="calc-result-row" style="border-top:1px solid var(--border);padding-top:8px;margin-top:4px">
      <span style="font-weight:700">월 현금흐름</span>
      <span class="calc-result-val" style="color:${color};font-size:16px">${cashflow >= 0 ? "+" : ""}${formatPrice(cashflow)}</span>
    </div>
  `;
}

export function initCalc() {
  // Attach event listeners to calculator buttons
  // These are typically attached via onclick in HTML, so this function
  // can be used to set up any programmatic listeners if needed.
}
