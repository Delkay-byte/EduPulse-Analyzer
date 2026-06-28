import { useState } from 'react';
import { Activity, Users, BookOpen, Shield } from 'lucide-react';
import { VerificationScreen } from './VerificationScreen';
import TemplateDownloadScreen from './TemplateDownloadScreen';
import DashboardView from './DashboardView';
import StudentListView from './StudentListView';

export default function HeadteacherPortal({ onSwitchToDirector }: { onSwitchToDirector: () => void }) {
  // Purely Frontend: No backend involved
  const [isVerified, setIsVerified] = useState(false);
  const [hasUploaded, setHasUploaded] = useState(false);
  const [activeSection, setActiveSection] = useState('Dashboard');

  // If not verified, show Verify Screen
  if (!isVerified) {
    return <VerificationScreen onVerify={() => setIsVerified(true)} />;
  }

  // If verified but no data yet, show Template/Upload Screen
  if (!hasUploaded) {
    return <TemplateDownloadScreen onUpload={() => setHasUploaded(true)} />;
  }

  // If both passed, show the actual Dashboard with sidebar
  return (
    <div className="flex h-screen bg-slate-950 text-white overflow-hidden font-sans">
      
      {/* SIDEBAR */}
      <aside className="w-64 bg-slate-900 border-r border-slate-800 flex flex-col">
        <div className="p-6 border-b border-slate-800">
          <h1 className="text-2xl font-black text-blue-500 tracking-tight">EduPulse</h1>
        </div>
        <nav className="flex-1 p-4 space-y-2 overflow-y-auto">
          <p className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-4 mt-2">Main Menu</p>
          <a 
            href="#" 
            onClick={() => setActiveSection('Dashboard')}
            className={`flex items-center gap-3 px-4 py-3 rounded-lg font-medium transition-colors ${
              activeSection === 'Dashboard' ? 'bg-blue-600/10 text-blue-500' : 'text-slate-400 hover:text-white hover:bg-slate-800'
            }`}
          >
            <Activity size={20} /> Dashboard
          </a>
          <a 
            href="#" 
            onClick={() => setActiveSection('Students')}
            className={`flex items-center gap-3 px-4 py-3 rounded-lg font-medium transition-colors ${
              activeSection === 'Students' ? 'bg-blue-600/10 text-blue-500' : 'text-slate-400 hover:text-white hover:bg-slate-800'
            }`}
          >
            <Users size={20} /> Students
          </a>
          <a href="#" className="flex items-center gap-3 text-slate-400 hover:text-white hover:bg-slate-800 px-4 py-3 rounded-lg transition-colors">
            <BookOpen size={20} /> Academics
          </a>
        </nav>
        <div className="p-4 border-t border-slate-800 flex items-center gap-3">
          <div className="w-10 h-10 bg-slate-700 rounded-full"></div>
          <div className="flex-1">
            <p className="text-sm font-bold">Sarah Jenkins</p>
            <p className="text-xs text-slate-400">Headteacher</p>
          </div>
          <button onClick={onSwitchToDirector} className="text-slate-400 hover:text-white transition-colors">
            <Shield size={18} />
          </button>
        </div>
      </aside>

      {/* MAIN CONTENT */}
      <main className="flex-1 overflow-y-auto bg-slate-950">
        {activeSection === 'Dashboard' && <DashboardView onSwitchToDirector={onSwitchToDirector} />}
        {activeSection === 'Students' && <StudentListView />}
      </main>
    </div>
  );
}
