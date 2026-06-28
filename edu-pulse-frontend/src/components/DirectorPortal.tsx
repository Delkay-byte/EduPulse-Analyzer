import { useState } from 'react';

function CircuitSetupGate({ onComplete }: { onComplete: () => void }) {
  return (
    <div className="h-full flex flex-col items-center justify-center p-8 text-center">
      <div className="bg-slate-900 border border-slate-800 p-12 rounded-2xl max-w-lg w-full">
        <h2 className="text-2xl font-black text-white mb-4">District Configuration Required</h2>
        <p className="text-slate-400 mb-8 text-sm">
          No active circuits file has been uploaded yet. The Director must upload the official circuits dataset before school onboarding can continue.
        </p>

        <div className="border-2 border-dashed border-slate-700 p-8 rounded-xl mb-6">
          <p className="text-slate-500 text-sm mb-4">Upload the official circuits CSV:</p>
          <button
            onClick={onComplete}
            className="bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 px-6 rounded-lg transition"
          >
            Upload Circuits CSV
          </button>
        </div>
      </div>
    </div>
  );
}

export default function DirectorPortal({ onSwitchToHeadteacher }: { onSwitchToHeadteacher: () => void }) {
  const [isCircuitReady, setIsCircuitReady] = useState(false);
  const [selectedCircuit, setSelectedCircuit] = useState('Akatsi North Circuit');
  const [activeTab, setActiveTab] = useState('Overview');

  const directorTabs = ['Overview', 'Leaderboard', 'Audit', 'Predict', 'Reports', 'Management', 'Intervention Strategy'];

  // Toast Notification Engine
  const [toast, setToast] = useState<{message: string, type: 'success' | 'error'} | null>(null);

  const showToast = (message: string, type: 'success' | 'error' = 'success') => {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  };

  return (
    <div className="flex h-screen bg-slate-900 text-slate-100 font-sans select-none">
      
      {/* Sidebar Layout */}
      <aside className="w-80 bg-slate-950 p-6 border-r border-slate-800 flex flex-col h-screen">
        {/* Branding & Role Context */}
        <div className="mb-8">
          <h1 className="text-xl font-black text-emerald-400">EduPulse</h1>
          <p className="text-xs text-slate-500 font-medium mt-1">
            District Portal
          </p>
        </div>

        {/* Main content scrollable area */}
        <div className="flex-1 overflow-y-auto space-y-8 pr-2 scrollbar-thin scrollbar-thumb-slate-800">

          {/* Navigation Links */}
          <div>
            <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-3">Navigation</h3>
            <div className="space-y-2">
              {directorTabs.map(tab => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`w-full text-left px-4 py-3 rounded-xl text-sm font-bold transition-all ${
                    activeTab === tab
                      ? 'bg-emerald-600 text-white shadow-lg'
                      : 'text-slate-400 hover:bg-slate-900'
                  }`}
                >
                  {tab}
                </button>
              ))}
            </div>
          </div>

          {/* Persistent Legend (The 'Flyers' & 'Diamonds') */}
          <div className="bg-slate-900/50 p-4 rounded-xl border border-slate-800">
            <h4 className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-3">Performance Legend</h4>
            <div className="space-y-2 text-[11px]">
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full bg-emerald-500"></div>
                <span className="text-slate-300">Diamonds (High Achievers)</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full bg-blue-500"></div>
                <span className="text-slate-300">Flyers (High Growth)</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 rounded-full bg-red-500"></div>
                <span className="text-slate-300">At-Risk (Intervention)</span>
              </div>
            </div>
          </div>
        </div>

        {/* Developer Toggle & Logout Control Center */}
        <div className="mt-6 pt-4 border-t border-slate-800 space-y-3">
          
          {/* DEV ONLY: Role Toggle */}
          <button 
            onClick={onSwitchToHeadteacher}
            className="w-full flex items-center justify-between px-3 py-2 bg-slate-900 border border-slate-700 hover:border-emerald-500 rounded-lg transition-colors group"
          >
            <span className="text-[10px] font-bold text-slate-400 group-hover:text-emerald-400 uppercase tracking-wider">
              Dev: Switch Role
            </span>
            <span className="text-xs">🔄</span>
          </button>

          {/* Production Logout Button */}
          <button 
            onClick={() => alert("Logout logic will clear tokens and route to Login Screen")}
            className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-red-500/10 hover:bg-red-500/20 border border-red-500/20 text-red-400 rounded-lg text-xs font-bold transition-colors"
          >
            <span>🚪</span> Logout
          </button>

          <div className="text-center pt-2">
            <p className="text-[9px] text-slate-600 tracking-wider uppercase font-bold">Powered by BloomCore Technologies</p>
          </div>
        </div>
      </aside>

      {/* Main Panel Frame */}
      <main className="flex-1 bg-slate-950 overflow-y-auto">

        {/* THE GATEKEEPER LOGIC */}
        {!isCircuitReady ? (
          <CircuitSetupGate onComplete={() => setIsCircuitReady(true)} />
        ) : (
          /* The Actual Director Tabs */
          <div className="p-8">

        {/* Global Action Header */}
        <header className="mb-8">
          <div className="flex justify-between items-end mb-6">
            <div>
              <span className="text-xs font-bold text-emerald-400/80 tracking-widest uppercase">Active Portal</span>
              <h2 className="text-3xl font-black tracking-tight text-white mt-0.5">Director Dashboard</h2>
            </div>

            <select
              value={selectedCircuit}
              onChange={(e) => setSelectedCircuit(e.target.value)}
              className="bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 text-sm font-semibold text-slate-300 focus:outline-none focus:border-emerald-500 cursor-pointer"
            >
              <option>All Circuits</option>
              <option>Akatsi North Circuit</option>
              <option>Ave Avenor Circuit</option>
            </select>
          </div>
        </header>

        {/* ---------------------------------------------------------------- */}
        {/* TAB 1: DIRECTOR OVERVIEW                                          */}
        {/* ---------------------------------------------------------------- */}
        {activeTab === 'Overview' && (
          <div className="space-y-6 animate-fadeIn">

            {/* Top 4 Metric Cards */}
            <div className="grid grid-cols-4 gap-4">
              <div className="bg-slate-950 p-5 rounded-2xl border border-slate-800 border-l-4 border-l-emerald-500 shadow-sm">
                <span className="text-[11px] font-bold text-slate-500 uppercase tracking-wider">👥 Total Students</span>
                <div className="text-3xl font-black text-white mt-1">415</div>
              </div>
              <div className="bg-slate-950 p-5 rounded-2xl border border-slate-800 border-l-4 border-l-blue-500 shadow-sm">
                <span className="text-[11px] font-bold text-slate-500 uppercase tracking-wider">📉 Avg Aggregate</span>
                <div className="text-3xl font-black text-white mt-1">345.01</div>
              </div>
              <div className="bg-slate-950 p-5 rounded-2xl border border-slate-800 border-l-4 border-l-indigo-500 shadow-sm">
                <span className="text-[11px] font-bold text-slate-500 uppercase tracking-wider">✅ Schools Reporting</span>
                <div className="text-3xl font-black text-white mt-1">20</div>
              </div>
              <div className="bg-slate-950 p-5 rounded-2xl border border-slate-800 border-l-4 border-l-amber-500 shadow-sm">
                <span className="text-[11px] font-bold text-slate-500 uppercase tracking-wider">🏆 Lead School</span>
                <div className="text-xl font-bold text-white mt-2 truncate">Dzodze JHS</div>
              </div>
            </div>

            {/* MIDDLE ROW */}
            <div className="grid grid-cols-2 gap-6">
              {/* School Coverage */}
              <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800">
                <h4 className="text-sm font-bold text-white flex items-center gap-2 mb-1">
                  🏫 School Upload Coverage
                </h4>
                <p className="text-[11px] text-slate-400 mb-6">Schools with uploaded BECE results in the current dataset.</p>

                <div className="grid grid-cols-3 gap-3">
                  <div className="bg-slate-900 p-4 rounded-xl border border-slate-800 text-center">
                    <div className="text-2xl font-black text-white">20</div>
                    <span className="text-[10px] text-slate-400 font-bold uppercase tracking-wider mt-1 block">With Data</span>
                  </div>
                  <div className="bg-emerald-950/30 p-4 rounded-xl border border-emerald-900/50 text-center">
                    <div className="text-2xl font-black text-emerald-400">415</div>
                    <span className="text-[10px] text-emerald-500 font-bold uppercase tracking-wider mt-1 block">Total Students</span>
                  </div>
                  <div className="bg-amber-950/30 p-4 rounded-xl border border-amber-900/50 text-center">
                    <div className="text-2xl font-black text-amber-400">5</div>
                    <span className="text-[10px] text-amber-500 font-bold uppercase tracking-wider mt-1 block">Circuits</span>
                  </div>
                </div>
              </div>

              {/* District Performance Overview */}
              <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800 flex flex-col justify-between">
                <div>
                  <h4 className="text-sm font-bold text-white flex items-center gap-2 mb-1">
                    📊 District Performance Overview
                  </h4>
                  <p className="text-[11px] text-slate-400 mb-6">Best 6 Aggregate distribution across all 20 schools.</p>
                </div>

                <div className="space-y-4">
                  {/* Performance Range */}
                  <div>
                    <div className="flex justify-between text-xs font-bold mb-1">
                      <span className="text-emerald-400">Best School</span>
                      <span className="text-slate-500">Dzodze JHS: 333.31</span>
                    </div>
                    <div className="w-full bg-slate-900 rounded-full h-3">
                      <div className="bg-emerald-500 h-3 rounded-full" style={{ width: '85%' }}></div>
                    </div>
                  </div>

                  {/* District Average */}
                  <div>
                    <div className="flex justify-between text-xs font-bold mb-1">
                      <span className="text-blue-400">District Average</span>
                      <span className="text-slate-500">345.01</span>
                    </div>
                    <div className="w-full bg-slate-900 rounded-full h-3">
                      <div className="bg-blue-500 h-3 rounded-full" style={{ width: '75%' }}></div>
                    </div>
                  </div>

                  {/* Worst School */}
                  <div>
                    <div className="flex justify-between text-xs font-bold mb-1">
                      <span className="text-amber-400">Needs Support</span>
                      <span className="text-slate-500">Anlo Awo JHS: 368.84</span>
                    </div>
                    <div className="w-full bg-slate-900 rounded-full h-3">
                      <div className="bg-amber-500 h-3 rounded-full" style={{ width: '60%' }}></div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ---------------------------------------------------------------- */}
        {/* TAB 2: LEADERBOARD                                                */}
        {/* ---------------------------------------------------------------- */}
        {activeTab === 'Leaderboard' && (
          <div className="space-y-6 animate-fadeIn">

            {/* TOP ROW: KPIs */}
            <div className="grid grid-cols-4 gap-4">
              {[
                { label: 'Total Students', val: '415', sub: 'With BECE Data', color: 'emerald' },
                { label: 'District Average', val: '345.01', sub: 'Best 6 Aggregate', color: 'blue' },
                { label: 'Top Performer', val: '333.31', sub: 'Dzodze JHS', color: 'amber' },
                { label: 'Schools Reporting', val: '20', sub: 'Of 20 Total', color: 'indigo' },
              ].map((kpi, i) => (
                <div key={i} className={`bg-slate-950 p-5 rounded-2xl border border-slate-800 border-l-4 border-l-${kpi.color}-500 shadow-sm`}>
                  <span className="text-[11px] font-bold text-slate-500 uppercase tracking-wider">{kpi.label}</span>
                  <div className="text-3xl font-black text-white mt-1">{kpi.val}</div>
                  <p className="text-xs text-slate-400 mt-2">{kpi.sub}</p>
                </div>
              ))}
            </div>

            {/* MIDDLE ROW: COMPARATIVE CHARTS */}
            <div className="grid grid-cols-2 gap-6">
              {/* Predicted vs Actual */}
              <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800">
                <h4 className="text-sm font-bold text-white mb-4 flex items-center gap-2">
                  🎯 Predicted vs. Actual BECE Results
                </h4>
                <div className="bg-slate-900 p-8 rounded-lg text-center text-slate-400">
                  <p className="text-sm mb-2">Scatter Plot: Predicted vs Actual BECE Scores</p>
                  <p className="text-xs text-slate-500">Each point represents a student's predicted aggregate (x-axis) vs actual BECE result (y-axis)</p>
                  <div className="mt-4 p-4 bg-slate-950 rounded border border-slate-800">
                    <p className="text-xs text-slate-400">Model Accuracy: R² = 0.89</p>
                    <p className="text-xs text-slate-400 mt-1">Correlation: Strong positive (0.94)</p>
                    <p className="text-xs text-slate-500 mt-2">Points clustering near diagonal line indicate accurate predictions</p>
                  </div>
                </div>
              </div>

              {/* Performance by Circuit/School */}
              <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800">
                <h4 className="text-sm font-bold text-white mb-4 flex items-center gap-2">
                  🗺️ Performance by Circuit
                </h4>
                <div className="bg-slate-900 p-8 rounded-lg text-center text-slate-400">
                  <p className="text-sm mb-2">Stacked Bar Chart: Average Best 6 Aggregate by Circuit</p>
                  <div className="mt-4 space-y-2">
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-slate-400 w-32">Ketu North</span>
                      <div className="flex-1 bg-slate-950 rounded h-6 relative">
                        <div className="bg-emerald-600 h-6 rounded flex items-center justify-end pr-2" style={{ width: '96%' }}>
                          <span className="text-[10px] text-white font-bold">333.95</span>
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-slate-400 w-32">Akatsi North</span>
                      <div className="flex-1 bg-slate-950 rounded h-6 relative">
                        <div className="bg-blue-600 h-6 rounded flex items-center justify-end pr-2" style={{ width: '97%' }}>
                          <span className="text-[10px] text-white font-bold">340.76</span>
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-slate-400 w-32">Keta</span>
                      <div className="flex-1 bg-slate-950 rounded h-6 relative">
                        <div className="bg-indigo-600 h-6 rounded flex items-center justify-end pr-2" style={{ width: '98%' }}>
                          <span className="text-[10px] text-white font-bold">341.42</span>
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-slate-400 w-32">Ketu South</span>
                      <div className="flex-1 bg-slate-950 rounded h-6 relative">
                        <div className="bg-amber-600 h-6 rounded flex items-center justify-end pr-2" style={{ width: '99%' }}>
                          <span className="text-[10px] text-white font-bold">343.77</span>
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-slate-400 w-32">Akatsi South</span>
                      <div className="flex-1 bg-slate-950 rounded h-6 relative">
                        <div className="bg-red-600 h-6 rounded flex items-center justify-end pr-2" style={{ width: '100%' }}>
                          <span className="text-[10px] text-white font-bold">347.31</span>
                        </div>
                      </div>
                    </div>
                  </div>
                  <p className="text-[10px] text-slate-500 mt-3">Lower aggregate indicates better performance</p>
                </div>
              </div>
            </div>

            {/* BOTTOM SECTION: RANKING LIST */}
            <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-xl font-bold text-white flex items-center gap-2">🏆 Akatsi Municipal Excellence Rankings</h3>
                <button className="bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-2 px-4 rounded-lg text-sm">
                  Download Ranking Report
                </button>
              </div>
              <p className="text-xs text-slate-400 mb-4">Ranking schools by average Best 6 Aggregate. Lower is better. Schools without uploads stay at zero with an awaiting-upload status.</p>

              {/* LEADERBOARD GRID */}
              <div className="space-y-3">
                <div className="space-y-2">
                  {[
                    { rank: 1, school: 'Dzodze JHS', circuit: 'Ketu North', agg: 333.31, students: 20 },
                    { rank: 2, school: 'Keta JHS', circuit: 'Keta', agg: 333.95, students: 24 },
                    { rank: 3, school: 'Kpetsu JHS', circuit: 'Akatsi North', agg: 334.51, students: 21 },
                    { rank: 4, school: 'Wute JHS', circuit: 'Akatsi North', agg: 336.17, students: 22 },
                    { rank: 5, school: 'Aflao JHS', circuit: 'Ketu South', agg: 336.81, students: 20 },
                    { rank: 6, school: "St. Mary's JHS", circuit: 'Akatsi South', agg: 339.79, students: 21 },
                    { rank: 7, school: 'Anyako JHS', circuit: 'Keta', agg: 340.42, students: 20 },
                    { rank: 8, school: 'Agorkpo JHS', circuit: 'Akatsi North', agg: 341.51, students: 23 },
                    { rank: 9, school: 'Tadzewu JHS', circuit: 'Ketu South', agg: 343.88, students: 23 },
                    { rank: 10, school: 'Denu JHS', circuit: 'Ketu South', agg: 344.04, students: 21 },
                  ].map((item) => (
                    <div key={item.rank} className="flex items-center gap-4 bg-slate-900 p-3 rounded hover:bg-slate-800 transition">
                      <div className="w-10 text-center font-black text-emerald-400">{item.rank}</div>
                      <div className="flex-1">
                        <div className="text-sm text-white font-semibold">{item.school}</div>
                        <div className="text-[10px] text-slate-400">{item.circuit} • {item.students} students</div>
                      </div>
                      <div className="w-20 text-right">
                        <div className="text-sm font-bold text-white">{item.agg.toFixed(2)}</div>
                        <div className="text-[10px] text-slate-400">avg aggregate</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ---------------------------------------------------------------- */}
        {/* TAB 3: STUDENT AUDIT                                              */}
        {/* ---------------------------------------------------------------- */}
        {activeTab === 'Audit' && (
          <div className="space-y-6">
            <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800">
              <h3 className="text-xl font-bold text-white">📋 Student Audit</h3>
              <p className="text-sm text-slate-400">Search and verify individual student records across all circuits.</p>

              {/* Filter Bar */}
              <div className="flex gap-3 mt-4">
                <input placeholder="Search Index No." className="flex-1 bg-slate-900 border border-slate-800 rounded-lg px-3 py-2 text-sm text-white" />
                <select className="bg-slate-900 border border-slate-800 rounded-lg px-3 py-2 text-sm text-white">
                  <option>All Circuits</option>
                  <option>Akatsi North</option>
                </select>
                <select className="bg-slate-900 border border-slate-800 rounded-lg px-3 py-2 text-sm text-white">
                  <option>All Schools</option>
                  <option>Awasive M/A JHS</option>
                </select>
              </div>

              {/* Results Table */}
              <div className="mt-6 overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="text-slate-400 text-xs uppercase tracking-wider">
                    <tr>
                      <th className="py-2">Index No.</th>
                      <th className="py-2">Name</th>
                      <th className="py-2">School</th>
                      <th className="py-2">Aggregate</th>
                      <th className="py-2">Status</th>
                    </tr>
                  </thead>
                  <tbody className="text-white">
                    <tr>
                      <td className="py-3">10501001</td>
                      <td className="py-3">John Doe</td>
                      <td className="py-3">Awasive M/A JHS</td>
                      <td className="py-3">12</td>
                      <td className="py-3 text-emerald-400 font-bold">PASS</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* ---------------------------------------------------------------- */}
        {/* TAB 4: AGGREGATE EXPLORER                                         */}
        {/* ---------------------------------------------------------------- */}
        {activeTab === 'Predict' && (
          <div className="space-y-6 animate-fadeIn">
            <div className="bg-slate-950 p-8 rounded-2xl border border-slate-800">
              <h3 className="text-2xl font-black text-white mb-2">📊 Aggregate Explorer</h3>
              <p className="text-sm text-slate-400 mb-8">
                Drill down from District to Circuit to School to identify performance gaps.
              </p>

              {/* Analysis View */}
              <div className="grid grid-cols-1 gap-6">
                {/* Performance Distribution Chart */}
                <div className="bg-slate-900 p-6 rounded-xl border border-slate-800">
                  <h4 className="text-sm font-bold text-white mb-4">Performance Distribution</h4>
                  <div className="bg-slate-950 p-12 rounded-lg text-center text-slate-400">
                    [Drill-down Analytics Chart: Average Aggregate Trends]
                  </div>
                </div>

                {/* Insight Cards */}
                <div className="grid grid-cols-3 gap-4">
                  <div className="bg-slate-900 p-4 rounded-xl border border-slate-800">
                    <p className="text-xs text-slate-400 mb-2">Predicted Avg Agg.</p>
                    <p className="text-2xl font-black text-emerald-400">14.2</p>
                  </div>
                  <div className="bg-slate-900 p-4 rounded-xl border border-slate-800">
                    <p className="text-xs text-slate-400 mb-2">Historical Trend</p>
                    <p className="text-2xl font-black text-blue-400">+2.4%</p>
                  </div>
                  <div className="bg-slate-900 p-4 rounded-xl border border-slate-800">
                    <p className="text-xs text-slate-400 mb-2">Risk Level</p>
                    <p className="text-2xl font-black text-amber-400">Moderate</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ---------------------------------------------------------------- */}
        {/* TAB 5: REPORTS                                                    */}
        {/* ---------------------------------------------------------------- */}
        {activeTab === 'Reports' && (
          <div className="space-y-8 animate-fadeIn">
            <div className="bg-slate-950 p-8 rounded-2xl border border-slate-800">
              <h3 className="text-xl font-bold text-white mb-6">📣 Communication Center</h3>
              
              <div className="grid grid-cols-2 gap-8">
                {/* Message Editor */}
                <div className="space-y-4">
                  <label className="block text-xs font-bold text-slate-500 uppercase">Target School</label>
                  <select className="w-full bg-slate-900 border border-slate-700 p-3 rounded-lg text-sm text-white">
                    <option>All Schools</option>
                    <option>Awasive M/A JHS</option>
                  </select>
                  
                  <label className="block text-xs font-bold text-slate-500 uppercase">Message Content</label>
                  <textarea 
                    rows={4} 
                    className="w-full bg-slate-900 border border-slate-700 p-3 rounded-lg text-sm text-white" 
                    placeholder="Draft your message here..."
                  />
                  
                  <button 
                    onClick={() => {
                      window.open(`https://wa.me/?text=Hello%20School%20Admin...`, '_blank');
                      showToast("WhatsApp Redirect Initiated");
                    }}
                    className="w-full bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-3 rounded-lg transition"
                  >
                    Launch WhatsApp Notice
                  </button>
                </div>

                {/* Calibration Logs */}
                <div className="bg-slate-900/50 p-6 rounded-xl border border-slate-800">
                  <h4 className="text-sm font-bold text-slate-300 mb-4">Calibration History</h4>
                  <div className="text-xs text-slate-500 space-y-3">
                     <p>• Calibration for Awasive saved (12:00 PM)</p>
                     <p>• Bulk warning sent to 8 schools (Yesterday)</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ---------------------------------------------------------------- */}
        {/* TAB 6: DATA MANAGEMENT                                           */}
        {/* ---------------------------------------------------------------- */}
        {activeTab === 'Management' && (
          <div className="space-y-6 animate-fadeIn">

            <header className="mb-6">
              <h3 className="text-2xl font-black text-white">⚙️ System & Sync Management</h3>
              <p className="text-sm text-slate-400">Audit district sync status and manage administrative controls.</p>
            </header>

            {/* Sync Health Audit Table */}
            <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800">
              <h4 className="text-lg font-bold text-white mb-4">📊 Pending Syncs</h4>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="text-slate-400 text-xs uppercase tracking-wider border-b border-slate-800">
                    <tr>
                      <th className="py-3">School</th>
                      <th className="py-3">Last Active</th>
                      <th className="py-3">Sync Status</th>
                      <th className="py-3">Action</th>
                    </tr>
                  </thead>
                  <tbody className="text-white">
                    {[
                      { name: 'Awasive JHS', last: '2 days ago', status: 'Stalled' },
                      { name: 'Wute JHS', last: '5 hours ago', status: 'Synced' },
                      { name: 'Akatsi Kpedu JHS', last: '12 hours ago', status: 'Synced' },
                    ].map((school, i) => (
                      <tr key={i} className="border-b border-slate-800 hover:bg-slate-900/50 transition">
                        <td className="py-3">{school.name}</td>
                        <td className="py-3 text-slate-400">{school.last}</td>
                        <td className="py-3">
                          <span className={`text-xs font-bold px-2 py-1 rounded ${
                            school.status === 'Stalled'
                              ? 'bg-red-950/30 text-red-400'
                              : 'bg-emerald-950/30 text-emerald-400'
                          }`}>
                            {school.status}
                          </span>
                        </td>
                        <td className="py-3">
                          {school.status === 'Stalled' && (
                            <button
                              onClick={() => showToast(`Pinged ${school.name} Headteacher via WhatsApp`)}
                              className="text-emerald-400 text-xs font-bold hover:underline transition"
                            >
                              Ping Head
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Security Key Banner */}
            <div className="bg-emerald-950/20 border border-emerald-900/50 p-4 rounded-xl flex items-center justify-between">
              <div>
                <h4 className="text-sm font-bold text-emerald-400 flex items-center gap-2">
                  🔑 District/Municipal Security Key for Headteacher Registration
                </h4>
                <p className="text-xs text-emerald-500/80 mt-1">Share this key with headteachers so they can create their accounts. Keep it secure.</p>
              </div>
              <div className="bg-slate-950 px-6 py-2 rounded-lg border border-emerald-500/30">
                <code className="text-lg font-mono font-bold text-emerald-400 tracking-wider">AKATSISOUTH-0000</code>
              </div>
            </div>

            {/* Upload Grids */}
            <div className="grid grid-cols-2 gap-6">

              {/* Column 1: Circuit Mapping */}
              <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800 flex flex-col">
                <div className="mb-6">
                  <h4 className="text-base font-bold text-white mb-1">🗺️ Update Circuit Mapping</h4>
                  <p className="text-xs text-slate-400">Re-upload the circuits CSV to update school-circuit mappings.</p>
                </div>

                <button className="w-full bg-slate-900 hover:bg-slate-800 text-slate-300 border border-slate-700 text-sm font-bold py-3 rounded-xl transition-colors mb-4 flex items-center justify-center gap-2">
                  <span>📥</span> Download Circuits Template (Excel)
                </button>

                <div className="flex-1 relative group mt-2">
                  <input
                    type="file"
                    accept=".xlsx,.csv"
                    className="absolute inset-0 w-full h-full opacity-0 cursor-pointer z-10"
                  />
                  <div className="h-full border-2 border-dashed border-slate-700 group-hover:border-blue-500 bg-slate-900/50 group-hover:bg-blue-950/20 rounded-xl flex flex-col items-center justify-center p-8 transition-all">
                    <div className="h-12 w-12 bg-slate-800 rounded-full flex items-center justify-center mb-3 group-hover:scale-110 transition-transform">
                      <span className="text-xl">🗂️</span>
                    </div>
                    <span className="text-sm font-bold text-slate-300 group-hover:text-blue-400">Upload completed circuits file</span>
                    <span className="text-xs text-slate-500 mt-1">Accepts .xlsx or .csv</span>
                  </div>
                </div>
              </div>

              {/* Column 2: WAEC PDF Sync */}
              <div className="bg-slate-950 p-6 rounded-2xl border border-slate-800 flex flex-col">
                <div className="mb-6">
                  <h4 className="text-base font-bold text-white mb-1">📄 Sync WAEC Result PDFs</h4>
                  <p className="text-xs text-slate-400">Upload official BECE PDF result files to extract and analyze student performance.</p>
                </div>

                <div className="flex-1 relative group">
                  <input
                    type="file"
                    accept=".pdf"
                    multiple
                    className="absolute inset-0 w-full h-full opacity-0 cursor-pointer z-10"
                  />
                  <div className="h-full border-2 border-dashed border-slate-700 group-hover:border-emerald-500 bg-slate-900/50 group-hover:bg-emerald-950/20 rounded-xl flex flex-col items-center justify-center p-8 transition-all">
                    <div className="h-12 w-12 bg-slate-800 rounded-full flex items-center justify-center mb-3 group-hover:scale-110 transition-transform">
                      <span className="text-xl">📑</span>
                    </div>
                    <span className="text-sm font-bold text-slate-300 group-hover:text-emerald-400">Upload WAEC PDF Results</span>
                    <span className="text-xs text-slate-500 mt-1">Supports multiple files simultaneously</span>
                  </div>
                </div>

                <button className="w-full bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-bold py-3 rounded-xl transition-colors mt-4 flex items-center justify-center gap-2 opacity-50 cursor-not-allowed">
                  <span>🔄</span> Process & Sync PDFs
                </button>
                <p className="text-[10px] text-center text-slate-500 mt-2">Button activates when PDFs are selected.</p>
              </div>

            </div>
          </div>
        )}

        {/* ---------------------------------------------------------------- */}
        {/* TAB 7: INTERVENTION STRATEGY                                      */}
        {/* ---------------------------------------------------------------- */}
        {activeTab === 'Intervention Strategy' && (
          <div className="space-y-6 animate-fadeIn">
            <div className="bg-slate-950 p-8 rounded-2xl border border-slate-800">
              <h3 className="text-2xl font-black text-white mb-2">📡 Strategy Broadcasting</h3>
              <p className="text-sm text-slate-400 mb-8">
                Publish directives directly to school portals and track Headteacher response to Circuit Pressure directives.
              </p>

              {/* Broadcast Directives */}
              <div className="space-y-4 mb-8">
                {[
                  { circuit: 'Akatsi North', directive: 'Weekend Remedial Classes' },
                  { circuit: 'Avenorpeme', directive: 'Attendance Enforcement' },
                  { circuit: 'Xavi', directive: 'Math Clinic Initiative' },
                ].map((item, i) => (
                  <div key={i} className="bg-slate-900 p-4 rounded-lg border border-slate-800 flex items-center justify-between">
                    <div>
                      <p className="text-sm font-bold text-white">{item.circuit}</p>
                      <p className="text-xs text-slate-400 mt-1">{item.directive}</p>
                    </div>
                    <button
                      onClick={() => showToast(`Broadcasted: ${item.directive} to all ${item.circuit} schools.`)}
                      className="bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold px-4 py-2 rounded-lg transition"
                    >
                      Broadcast Directive
                    </button>
                  </div>
                ))}
              </div>

              {/* Strategy Monitoring Table */}
              <h4 className="text-lg font-bold text-white mb-4">📊 Response Tracking</h4>
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead className="text-slate-400 text-xs uppercase tracking-wider border-b border-slate-800">
                    <tr>
                      <th className="py-3">Circuit</th>
                      <th className="py-3">Directive</th>
                      <th className="py-3">Status</th>
                      <th className="py-3">Impact Goal</th>
                    </tr>
                  </thead>
                  <tbody className="text-white">
                    {[
                      { circuit: 'Akatsi North', directive: 'Weekend Remedial Classes', status: 'In Progress', goal: '15% Agg. Improvement' },
                      { circuit: 'Avenorpeme', directive: 'Attendance Enforcement', status: 'Pending', goal: '95% Attendance' },
                      { circuit: 'Xavi', directive: 'Math Clinic', status: 'Completed', goal: 'District Avg' },
                    ].map((row, i) => (
                      <tr key={i} className="border-b border-slate-800 hover:bg-slate-900/50 transition">
                        <td className="py-3">{row.circuit}</td>
                        <td className="py-3 text-slate-300">{row.directive}</td>
                        <td className="py-3">
                          <span className={`text-xs font-bold px-2 py-1 rounded ${
                            row.status === 'In Progress'
                              ? 'bg-blue-950/30 text-blue-400'
                              : row.status === 'Pending'
                              ? 'bg-amber-950/30 text-amber-400'
                              : 'bg-emerald-950/30 text-emerald-400'
                          }`}>
                            {row.status}
                          </span>
                        </td>
                        <td className="py-3 text-slate-300">{row.goal}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* Temporary Fallback for the remaining unbuilt tabs */}
        {activeTab !== 'Overview' && activeTab !== 'Management' && activeTab !== 'Leaderboard' && activeTab !== 'Audit' && activeTab !== 'Predict' && activeTab !== 'Reports' && activeTab !== 'Intervention Strategy' && (
          <div className="bg-slate-950 p-12 rounded-2xl border border-dashed border-slate-800 text-center animate-fadeIn">
            <h3 className="text-xl font-bold text-slate-400 mb-2">{activeTab} Workspace</h3>
            <p className="text-sm text-slate-500">Module under construction.</p>
          </div>
        )}

          </div>
        )}

      </main>

      {/* Toast Notification Overlay */}
      {toast && (
        <div className={`fixed bottom-6 right-6 px-6 py-3 rounded-xl border font-bold text-sm shadow-2xl animate-slideIn ${
          toast.type === 'success' 
            ? 'bg-emerald-950 border-emerald-500 text-emerald-400' 
            : 'bg-red-950 border-red-500 text-red-400'
        }`}>
          {toast.message}
        </div>
      )}
    </div>
  );
}
