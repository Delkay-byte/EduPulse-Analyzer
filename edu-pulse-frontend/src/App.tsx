import { useState } from 'react';
import DirectorPortal from './components/DirectorPortal';
import HeadteacherPortal from './components/HeadteacherPortal';
import LoginScreen from './components/LoginScreen';

export default function App() {
  const [role, setRole] = useState<'Director' | 'Headteacher' | null>(null);

  // Hard Split: Only one of these will exist in the DOM at a time.
  return (
    <div className="h-screen w-screen overflow-hidden">
      {!role ? (
        <LoginScreen onLogin={(r) => setRole(r)} />
      ) : role === 'Director' ? (
        <DirectorPortal onSwitchToHeadteacher={() => setRole('Headteacher')} />
      ) : (
        <HeadteacherPortal onSwitchToDirector={() => setRole('Director')} />
      )}
    </div>
  );
}